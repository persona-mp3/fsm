package main

import (
	"flag"
	"fmt"
	"fsm/dock"

	"github.com/BurntSushi/toml"
)

var clusterConfig = "cluster_config.toml"

func readConfig() (*dock.NodeConfig, error) {
	flag.StringVar(&clusterConfig, "config", clusterConfig, "cluster-config")
	flag.Parse()

	config := dock.NodeConfig{}
	meta, err := toml.DecodeFile(clusterConfig, &config)
	if err != nil {
		fmt.Printf("undecoded-keys: %+v\n", meta.Undecoded())
		return nil, fmt.Errorf("could not parse cluster config: %w", err)
	}
	return &config, nil
}
