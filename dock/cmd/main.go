package main

import (
	"flag"
	"fmt"
	"fsm/dock"
	"os"

	"github.com/BurntSushi/toml"
)

var configPath = "deploy.toml"

func main() {
	flag.StringVar(&configPath, "config", configPath, "deploy config to generate config")
	flag.Parse()

	deployConfig := dock.DeployCfg{}
	fmt.Println("config-file::", configPath)
	
	meta, err := toml.DecodeFile(configPath, &deployConfig)
	if err != nil {
		fmt.Println("could not decoded config file.", err)
		fmt.Printf("undecoded keys: %+v\n", meta.Undecoded())
		os.Exit(1)
	}

	if _, err := dock.GenerateConfigFile(deployConfig); err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}
