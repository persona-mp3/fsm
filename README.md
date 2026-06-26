Raft Implementation Algorithm
--
Implementing the Raft Algorithm for Distributed Systems. A working version with full
`Leader Election` is on the `feat/leader-election` branch. This branch is currenlty a 
refactor with more docs and a custom logger for easier debugging


Configure cluster
---
Use `config_cluster.toml`
The simulation client also shares the same config, but later on,  plan on having 
a seperate one instead


Run cluster
---
```bash
go run .
```

Run simulation
---
```bash
# enforces the cluster to acknowledge it as the leader
go run simulation/single-leader.go 
```


