### Configuration
By default a cluster of three nodes are created for a consensus system to work. It reads the `config_cluster.toml`
file to parse the config and recreate the nodes. You can extend the number of nodes you want in cluster by providing 
the local addresses you want them to bind and listen to 


To run a cluster of 5 nodes on ports 4001-4005, simply add this to the config_cluster.toml
```toml
addresses = ["localhost:4001", "localhost:4002", "localhost:4003", "localhost:4004", "localhost:4005"]
```

To run an isolated node where it cannot contact other nodes and provide at least one port for a peer
```toml
[cluster_settings.topology]
type = "node"
ports = [5000, 5001, 5002, 5003]
```

To generate a specific kind of cluster please see the `deploy.toml` for more information


