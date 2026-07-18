package dock

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"golang.org/x/crypto/ssh"
)

// dock config deploy.toml
var configFile = "deploy.toml"

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

func GenerateConfigFile(cfg DeployCfg) (TopologyType, error) {
	content, err := os.ReadFile(configFile)
	if err != nil {
		fmt.Println("could not read config file. ", err)
		os.Exit(1)
	}

	deployCfg := DeployCfg{}
	_, err = toml.Decode(fmt.Sprintf("%s", content), &deployCfg)
	if err != nil {
		fmt.Println("could not decode toml file. ", err)
		os.Exit(1)
	}

	fmt.Println("parsed config sucessfully creating node configurations")

	topologyType := deployCfg.ClusterSettings.Topology.Type
	switch topologyType {

	case TopologyNode:
		configs := createTopologySingleConfig(deployCfg)
		sshClients := []*ssh.Client{}

		for _, nodeCfg := range configs {
			nodeCfg.writeToConfigFile()
			if deployCfg.Deploy {
				client, err := nodeCfg.connect()
				// bail if any dial fails
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}

				sshClients = append(sshClients, client)
			}

			fmt.Println("generated config for node", nodeCfg.Id)
		}
		defer func() {
			for _, client := range sshClients {
				err := client.Close()
				if err != nil {
					fmt.Println(err)
				}
			}
		}()

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
