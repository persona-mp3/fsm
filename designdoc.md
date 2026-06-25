From previous iterations, the architecture was split into three or more components
unexpected and unknowingly.
1. Network Layer/Server
2. MiddleMan/Intermediary Layer/Node
3. RaftState 


After some time, I decided to takeout the middleman, now each `State` is the node
It recvs rpc requests directly from the Server and returns responses via channels

At this point, it reduces the concurrency overhead and focuses on making a 
`Final State Machine`. I've made it strict that weird edge cases I think could happen
or are very odd, for example a `Candidate` going into `Candidate` state, would cause a 
panic, or sending the wrong payload response for an rpcRequest will cause a panic

To bolster it up and force constraints, I made sure that only the `incoming` channel 
in the whole program was buffered by 1. So when the server recvs an rpc, it fowards it
into the node itself, and then the node/state sends the reponse back to the server

RWMutexes are used when updating the `State` of the Node. 
```go
func (r *Raft) updateState(raftState RaftState) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.state = raftState
}
```

```go
func (r *Raft) getCurrentState() RaftState {
    r.mu.RLock()
    state := r.raftState
    defer r.mu.RUnlock()
    return state
}
```

However, I don't know if RWMutexes will be a problem if I wanted to go into 
perf. Because looking at the source code impl, (still bizzare to me)
it has 2 locks, so it looks like taking a Lock holds two locks.But thats for another time 

Writes to the `Term` is atomic and only handled by the `Candidate` state
```go
func (r *Raft) incrementTerm() {
    r.term.Add(1)
}
```


The logging format needs to also change, but I like the direction it's going to
I'd want to activate logging, strucutured logging and levels in this next refactor
and have a config.toml file that can just be updated, right now, the logging is just 
using the logPackage and I'm just running the configs

TODO in addition to commit ed993481cb8233858d483804d080b89fce9b56ee 
---
We also need to look for a way to set the timers appropriately

