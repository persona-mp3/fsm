// dock is a deployment tooling for fsm. It parses 'deploy.toml' or any config
// passed to it via the '--config' flag to generate a config for the fsm engine to run
package dock

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	scp "github.com/bramvdbogaerde/go-scp"
	"golang.org/x/crypto/ssh"
)

type Machine struct {
	// Addr this node will listen on for incoming rpcs
	IP   string `toml:"ip"`
	Host string `toml:"host"`

	// ListenPort is the port the node will bind to for incoming rpcs
	ListenPort int `toml:"listen_port"`

	// PprofPort is an open http/https endpoint that exposes run time details from pprof
	PprofPort int `toml:"pprof_port"`

	SSHPath   string `toml:"ssh_path"`
	LoginUser string `toml:"login_user"`
}

type TopologyType string

const (
	// Run only one node on this machine
	TopologyNode TopologyType = "node"
	// Run all nodes as a single process on the same machine
	TopologyCluster TopologyType = "cluster"
	// Run all nodes as seperate processes on the same machine
	TopologyIsolated TopologyType = "isolated"
)

type Topology struct {
	Type  TopologyType `toml:"type"`
	Ports []int        `toml:"ports"`
}

type ClusterSettings struct {
	Heartbeat   int      `toml:"heartbeat"`
	MinInterval int      `toml:"min_interval"`
	MaxInterval int      `toml:"max_interval"`
	Topology    Topology `toml:"topology"`
}

type DeployCfg struct {
	Machines        []Machine
	Deploy          bool            `toml:"deploy"`
	ClusterSettings ClusterSettings `toml:"cluster_settings"`
}

type NodeConfig struct {
	Id              int             `toml:"id"`
	Info            string          `toml:"info"`
	Listen          string          `toml:"listen"`
	PprofAddr       string          `toml:"pprof_addr"`
	Peers           []string        `toml:"peers"`
	ClusterSettings ClusterSettings `toml:"cluster_settings"`

	// shh configutations are not written in the nodes configuration file
	sshPath   string
	loginUser string
	ip        string
}

func GenerateConfigFile(deployCfg DeployCfg) (TopologyType, error) {
	fmt.Println("parsed config sucessfully creating node configurations")

	topologyType := deployCfg.ClusterSettings.Topology.Type
	switch topologyType {

	case TopologyNode:
		configs := createTopologySingleConfig(deployCfg)

		for _, nodeCfg := range configs {
			nodeCfg.writeToConfigFile()
			fmt.Println("generated config for node", nodeCfg.Id)
		}

		if !deployCfg.Deploy {
			return topologyType, nil
		}

		sshClients := []*ssh.Client{}
		defer func() {
			for _, client := range sshClients {
				err := client.Close()
				if err != nil {
					fmt.Println(err)
				}
			}
		}()
		for _, node := range configs {
			client, err := node.connect()
			if err != nil {
				log.Fatalf("could not connect for %s. %s", node.ip, err)
			}
			sshClients = append(sshClients, client)
		}

		wg := sync.WaitGroup{}
		for idx, client := range sshClients {
			node := configs[idx]
			configFileName := fmt.Sprintf("config-%d.toml", node.Id)
			configFile, err := os.Open(configFileName)
			if err != nil {
				log.Fatal(err)
			}

			wg.Go(func() {
				if err = startUp(client, configFile); err != nil {
					log.Fatal(err)
				}
			})
		}

		wg.Wait()
		// write configuration files
	case TopologyCluster:
		config := newClusterTopologyConfig(deployCfg)
		f, err := os.Create("cluster-config.toml")
		if err != nil {
			return topologyType, err
		}
		content, err := toml.Marshal(config)
		if err != nil {
			return topologyType, fmt.Errorf("could not marhsall cluster-topology config: %w", err)
		}

		defer f.Close()

		if _, err := fmt.Fprintf(f, "%s", content); err != nil {
			return topologyType, fmt.Errorf("could not write cluster-topology config to file: %w", err)
		}
		fmt.Println("generated cluster config sucessfully")
	case TopologyIsolated:
		panic("isolated topology not yet implemented")
	default:
		fmt.Printf("unsuppoted topology type: %s\n ", topologyType)
		os.Exit(1)
	}

	return topologyType, nil
}

func (n *NodeConfig) writeToConfigFile() error {
	content, err := toml.Marshal(n)
	if err != nil {
		return fmt.Errorf("could not marshal node configuration. %d, %w ", n.Id, err)
	}

	f, err := os.Create(fmt.Sprintf("config-%d.toml", n.Id))
	defer func() {
		err := f.Close()
		if err != nil {
			fmt.Println("   WARN: could not close config file because it had already been closed: ", err)
		}
	}()

	if err != nil {
		return fmt.Errorf("could not create to config file. %d, %w ", n.Id, err)
	}

	if _, err := fmt.Fprintf(f, "%s", content); err != nil {
		return fmt.Errorf("could not write to config file. %d, %w ", n.Id, err)
	}

	return nil
}

func (n *NodeConfig) connect() (*ssh.Client, error) {
	content, err := os.ReadFile(n.sshPath)
	if err != nil {
		return nil, fmt.Errorf("could not read key: %s. %w", n.sshPath, err)
	}
	signer, err := ssh.ParsePrivateKey(content)
	if err != nil {
		return nil, fmt.Errorf("could not parse private key: %s. %w", n.sshPath, err)
	}

	clientConfig := ssh.ClientConfig{
		User: n.loginUser,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", n.ip), &clientConfig)
	if err != nil {
		return nil, fmt.Errorf("could not dial via ssh: %s, %w", n.sshPath, err)
	}

	return conn, nil
}

func startUp(client *ssh.Client, configFile *os.File) error {
	fmt.Println("running system update and installing openjdk-21")
	runSystemUpdateAndInstall := "apt update && apt install -y openjdk-21-jdk"
	if err := run(client, runSystemUpdateAndInstall, true); err != nil {
		return fmt.Errorf("could not runSystemUpdateAndInstall. reason: %w", err)
	}

	scpClient, err := scp.NewClientBySSH(client)
	if err != nil {
		return fmt.Errorf("failed to create scpClient. %w", err)
	}

	jarPath := "./jkvs/jkvs/target/jkvs-server.jar"
	jarFile, err := os.Open(jarPath)
	if err != nil {
		return fmt.Errorf("could not open jar file at: %w ", err)
	}

	targetFile := "/app/jkvs-server.jar"

	jarInfo, _ := jarFile.Stat()
	now := time.Now()
	fmt.Println("copying jar file over to host")
	if err := scpClient.Copy(context.Background(), jarFile, targetFile, "0665", jarInfo.Size()); err != nil {
		return fmt.Errorf("could not copy over jarFile:  %w ", err)
	}
	fmt.Printf(" >> scp took [%s]\n\n", time.Since(now).String())

	// copy over FSM binary
	fsm, err := os.Open("fsm")
	if err != nil {
		return fmt.Errorf("could not open fsm binary %w", err)
	}

	fileInfo, _ := fsm.Stat()
	fmt.Println("copying over fsm")
	now = time.Now()
	err = scpClient.Copy(context.Background(), fsm, "/app/fsm", "0655", fileInfo.Size())
	if err != nil {
		return fmt.Errorf("could not copy over fsm %w", err)
	}
	fmt.Printf(" >> scp took [%s]\n\n", time.Since(now).String())

	// copy cluster config
	configFileSizeInfo, _ := configFile.Stat()

	fmt.Println("copying over cluster config")
	err = scpClient.Copy(context.Background(), configFile, "/app/config.toml", "0655", configFileSizeInfo.Size())
	if err != nil {
		return fmt.Errorf("could not copy over cluster config %w", err)
	}
	fmt.Printf(" >> scp took [%s]\n\n", time.Since(now).String())

	// err = scpClient.Copy(context.Background(), configFile, "/app/config.toml", "0665", fileInfo.Size())

	fmt.Println("running jkvs application")
	runJKVS := "java -jar /app/jkvs-server.jar &"
	if err := run(client, runJKVS, true); err != nil {
		return fmt.Errorf("could not runJKVS. reason: %w", err)
	}

	// running FSM
	fmt.Println("running topology node")
	runFSMBinary := "./fsm --topology node --config config.toml &"
	if err := run(client, runFSMBinary, true); err != nil {
		return fmt.Errorf("could not runFSMBinary. reason: %w", err)
	}

	return nil
}

func run(client *ssh.Client, cmd string, showOut bool) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	start := time.Now()
	content, err := session.CombinedOutput(cmd)
	if err != nil {
		return err
	}
	done := time.Since(start)

	if showOut {
		fmt.Println(string(content))
		fmt.Printf("\n  $[%s] took :%s\n", cmd, done.String())
	}

	// if err := session.Run(cmd); err != nil {
	// 	return err
	// }
	return nil

}
