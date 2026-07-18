package dock

import (
	"fmt"
	"os"
	"time"

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

type Mode string

const (
	// Run only one node on this machine
	ModeSingle Mode = "single"
	// Run all nodes as a single process on the same machine
	ModeCluster Mode = "cluster"
	// Run all nodes as seperate processes on the same machine
	ModeIsolated Mode = "isolated"
)

type ClusterSettings struct {
	Heartbeat   int  `toml:"heartbeat"`
	MinInterval int  `toml:"min_interval"`
	MaxInterval int  `toml:"max_interval"`
	Mode        Mode `toml:"mode"`
}

type Deploy struct {
	Machines        []Machine
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

// func main() {
// 	flag.StringVar(&configFile, "config", configFile, "reads the deployment toml config file and parses")
// 	flag.Parse()
//
// 	content, err := os.ReadFile(configFile)
// 	if err != nil {
// 		fmt.Println("could not read config file. ", err)
// 		os.Exit(1)
// 	}
//
// 	deploy := Deploy{}
// 	_, err = toml.Decode(fmt.Sprintf("%s", content), &deploy)
// 	if err != nil {
// 		fmt.Println("could not decode toml file. ", err)
// 		os.Exit(1)
// 	}
//
// 	fmt.Println("parsed config sucessfully creating node configurations")
//
// 	configs := createNodeConfig(deploy)
// 	sshClients := []*ssh.Client{}
//
// 	defer func() {
// 		for _, client := range sshClients {
// 			err := client.Close()
// 			if err != nil {
// 				fmt.Println(err)
// 			}
// 		}
// 	}()
//
// 	// write configuration files
//
// 	for _, nodeCfg := range configs {
// 		nodeCfg.writeToConfigFile()
// 		client, err := nodeCfg.connect()
// 		// bail if any dial fails
// 		if err != nil {
// 			fmt.Println(err)
// 			os.Exit(1)
// 		}
//
// 		sshClients = append(sshClients, client)
// 	}
//
// }

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

func createNodeConfig(deploy Deploy) []NodeConfig {
	allPeersAddr := []string{}
	for _, machine := range deploy.Machines {
		// each peer will have ip:port
		addr := fmt.Sprintf("%s:%d", machine.IP, machine.ListenPort)
		allPeersAddr = append(allPeersAddr, addr)
	}

	nodeConfigs := []NodeConfig{}
	for idx, machine := range deploy.Machines {
		nodeConfig := NodeConfig{}
		nodeConfig.Id = idx + 1

		nodeConfig.Listen = fmt.Sprintf("%s:%d", machine.Host, machine.ListenPort)
		nodeConfig.PprofAddr = fmt.Sprintf("%s:%d", machine.Host, machine.PprofPort)

		remove := idx
		// creates a copy of the original slice, and get's all the peer addresses of
		// before this node in allPeersAddr
		peers := append([]string{}, allPeersAddr[:remove]...)
		// and all peers after this node's ipAddr
		peers = append(peers, allPeersAddr[remove+1:]...)
		nodeConfig.Peers = peers

		// cluster configuration
		nodeConfig.ClusterSettings.Heartbeat = deploy.ClusterSettings.Heartbeat
		nodeConfig.ClusterSettings.MinInterval = deploy.ClusterSettings.MinInterval
		nodeConfig.ClusterSettings.MaxInterval = deploy.ClusterSettings.MaxInterval
		nodeConfig.ClusterSettings.Mode = deploy.ClusterSettings.Mode

		// ssh configuration
		nodeConfig.sshPath = machine.SSHPath
		nodeConfig.loginUser = machine.LoginUser
		nodeConfig.ip = machine.IP

		info := fmt.Sprintf("id: %d, ip: %s, generatedAt: %s, by: dock",
			nodeConfig.Id, nodeConfig.ip, time.Now().Format("2006-01-02"),
		)

		nodeConfig.Info = info
		nodeConfigs = append(nodeConfigs, nodeConfig)
	}

	return nodeConfigs
}

