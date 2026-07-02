package utils

import (
	"fmt"
	rlog "fsm/raftlogger"
	"github.com/BurntSushi/toml"
	"os"
	"strings"
)

const (
	defaultSimulationConfigPath = "cluster_config.toml"
)

type Simulation struct {
	// TotalNodes is the total number of nodes that the cluster will start up. Default is 3
	TotalNodes int

	// Addresses contains ip addresses for where each node will start. Defaults are all
	// on localhost, from port 4000 to 4002.
	Addresses []string

	// TODO: ForceTerm  needs to be higher than the term a node or cluster is in
	ForceTerm uint64

	ForceDurationMs int

	log rlog.RLogger

	// TODO: Persists tells the simulation to increment the term till it get's accepted
	Persist bool
}

func ParseConfig(path string) (*Simulation, error) {
	if len(strings.ReplaceAll(path, " ", "")) == 0 {
		path = defaultSimulationConfigPath
		fmt.Printf("using default cluster config for simulation\n\n")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not load config. %w ", err)
	}

	cfg := DefaultSimulation()
	if _, err := toml.Decode(string(content), cfg); err != nil {
		fmt.Println("could not parse config file: ", err)
		fmt.Println("using defaults")
		cfg = DefaultSimulation()
	}

	return cfg, nil

}

func DefaultSimulation() *Simulation {
	l := rlog.NewHumaneLogger("0", "simulation", 0, os.Stdout)
	addrs := []string{
		"localhost:4000",
	}

	totalNodes := 3

	return &Simulation{
		TotalNodes: totalNodes,
		Addresses:  addrs,
		log:        l,
	}
}
