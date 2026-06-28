Raft Implementation Algorithm
--
This implementation follows the paper [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
called Raft.  The aim of this project is to
- understand distributed systems, raft consensus algorithm and implement it
- use as the consensus layer for my database, [jkvs](https://github.com/persona-mp3/jkvs) a concurrent 
key-value database that supports WAL, log compaction and single-writer multiple reader concurrency model.

The current implementation of this uses a custom logger I wrote for easier debugging, so the logs will look `unique`, later 
on structured logging will be implemented

Run cluster
---
By default a cluster of three nodes are created for a consensus system to work. It reads the `config_cluster.toml`
file to parse the config and recreate the nodes. You can extend the number of nodes you want in cluster by providing 
the local addresses you want them to bind and listen to 

To run the application

```bash
go run --race .
```

Run simulation
---
These serve to assert the behaviour we expect as writing tests are hard without introducing dataraces on the test itself or messing 
with the internal concurrent structure of the code. Another pending refactor will happen to be able to inject fake Clocks and networks. 
The simulations help to describe what we expect from a healthy cluster or a single node by interrupting it. At the moment the simulations share 
configs with the Cluster itself, `cluster_config.toml`, later on, we plan on adding more configuration 
options for the simulations
```bash
# enforces the cluster to acknowledge it as the leader, you can set this 
# under the force_term attribute in the cluster_config.toml
go run simulation/single-leader.go 
```


Profiling and Monitoring
---
- Every 2seconds, the program prints the number of goroutines running. This should not continously increase but remain steady overtime
- - Visit [http://localhost:6061/debug/pprof/](http://localhost:6061/debug/pprof/) while the appilication is running, it uses go's
[net/http/pprof](https://go.dev/blog/pprof). A config option will be added for this later on


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


