Raft Implementation Algorithm
--
This implementation follows the paper [In Search of an Understandable Concensus Algorithm](https://raft.github.io/raft.pdf)
called Raft.  The aim of this repo is to understand distributed systems, raft concensus algorithm and implement it 
and also use as the consensus layer for a database I built [jkvs](https://github.com/persona-mp3/jkvs) a concurrent 
key-value database that supports WAL, log compaction and single-writer multiple reader concurrency model.

The current implementation of this uses a custom logger I wrote for easier debugging, so the logs will look unique, later 
on structured learning will be implemented

Run cluster
---
By default a cluster of three nodes are created for a concensus system to work. It reads the `config_cluster.toml`
file to parse the config and recreate the nodes. You can extend the number of nodes you want in cluster by providing 
the local addresses you want them to bind and listen to 

To run the application

```bash
go run .
```

Run simulation
---
These serve to assert the behaviour we expect as writing tests are hard, we can often describe what we 
expect from a healthy cluster and interrupt it with these simulations. At the moment they share 
configs with the Cluster itself, `cluster_config.toml`, later on, we plan on adding more configuration 
options for the simulations
```bash
# enforces the cluster to acknowledge it as the leader, you can set this 
# under the force_term attribute in the cluster_config.toml
go run simulation/single-leader.go 
```


Profiling and Monitoring
---
Visit [http://localhost:6061/debug/pprof/](http://localhost:6061/debug/pprof/) while the appilication is running, it uses go's
[net/http/pprof](https://go.dev/blog/pprof).


Next 
---
- [ ] Log replication across the cluster
- [ ] Control plane for killing specific nodes


Todos
---
- [X] Implement custom logger
- [X] Adding tests
- [X] Implementing simulation testing
- [X] Starting cluster from a config file,  `cluster_config.toml` with default number of nodes 3
- [X] Leader Election
    - [X] Reimplement Follower 
    - [X] Reimplement Leader
    - [X] Reimplement Candidate


