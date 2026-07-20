package dock

import (
	"fmt"
	"time"
)

func createTopologySingleConfig(deploy DeployCfg) []NodeConfig {
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
		nodeConfig.ClusterSettings = deploy.ClusterSettings

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

type SingleClusterConfig struct {
	Peers           []string        `toml:"peers"`
	PprofAddr       string          `toml:"pprof_addr"`
	ClusterSettings ClusterSettings `toml:"cluster_settings"`
}

func newClusterTopologyConfig(deploy DeployCfg) SingleClusterConfig {
	const host = "localhost"

	clusterSettings := deploy.ClusterSettings
	nodeCfg := SingleClusterConfig{}
	peers := []string{}

	for _, port := range clusterSettings.Topology.Ports {
		addr := fmt.Sprintf("%s:%d", host, port)
		peers = append(peers, addr)
	}

	nodeCfg.Peers = peers
	nodeCfg.PprofAddr = fmt.Sprintf("%s:%d", host, 6061)
	nodeCfg.ClusterSettings = clusterSettings
	return nodeCfg
}
