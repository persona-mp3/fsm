Raft Implementation Algorithm
--
This implementation follows the paper [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
called Raft.  The aim of this project is to build a distributed database. The underlying database is a custom
database built in Java. It was originally inspired by the PingCaps TiKV talent plan, but I adopted other methods 
along the way instead of following the full spec.  For more information about the database, jkvs,
visit [JKVS Database](https://github.com/persona-mp3/jkvs.git)


Run a cluster
---
### Configuration
By default a cluster of three nodes are created for a consensus system to work. It reads the `config_cluster.toml`
file to parse the config and recreate the nodes. You can extend the number of nodes you want in cluster by providing 
the local addresses you want them to bind and listen to 


To run a cluster of 5 nodes on ports 4001-4005, simply add this to the config_cluster.toml
```toml
addresses = ["localhost:4001", "localhost:4002", "localhost:4003", "localhost:4004", "localhost:4005"]
```

To run an isolated node where it cannot contact other nodes
```toml
single = true
```
> Note that this config option is bound to change later on to support different modes


### Run the application
```bash
go run .

```

Observability and Tooling
---
### pprof integration
You can monitor the cluster at [http://localhost:6061/debug/pprof/](http://localhost:6061/debug/pprof/) as it 
uses Go's built in pprof library

### Custom loggers
The logger used at this current state of the application serves for easy debugging. This is 
in no way designed to be performant and will be ripped out later on in favor for structured logging.
Depending on the number of nodes running in the cluster, the same number of log files for each node 
will be created in the current directory. If you have 5 nodes running, their respective log files will be
named with prefix `log-file-node-id`

To be able to understand the logs,
```
[time.ms] (nodeId:owner:term) Information
```

An example logs as such
```
21:48:40.123340 (1:node:0) successfully connected to database
```
Should be read as 
`This node with an id of 1, the current point of execution is within the node component of the application, and is in it's 0th term`

```
21:48:41.543572 (1:leader:2) leader state transitioned successfully diagnostics: { id: 1, term: 2, state: Leader, votedFor|leader: , logs:  }
```
Should be read as 
`This node with an id of 1, is the leader for the term and it just started being a leader`

There's more to this as logs are attached to the function name of their caller


Run a simulation
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



Architecture
---
FSM sits between the clients and the database. 
A client sends a `CommandRPC` via TCP (eventually other protocols will be suppoted). 
If the node in the cluster that receives this commandRPC is the Leader, it sends 
this command to the whole of the cluster to be replicated. It only waits for a majority of the 
Followers to say that they've appended this log, before proceeding to execute the command locally 
in it's database. The Node does this by sending the command to the database via tcp and 
a simple binary protocol, that [jkvs](https://github.com/persona-mp3/jkvs.git) uses. This 
is typically a prefixed 4byte header for a packet and a `\r\n` delimiter for parts of the payload.
This was chosen for the database because JSON marshalling is expensive. At this point of development 
using Protobufs were over-engineering, especially since the protocol wasn't complex and is very simple.
When the database replies, the leader forwards the response from the underlying database back to the client

If the Leader does not receive a majority before a hard set timeout of 180ms, it fails to 
execute the command of the client. 

Constraints
---
- Latency: Sending a command to  a majority of a cluster takes a lot of time, and causes noticeable 
latency from the client's perspective. So waiting for a quorum of Followers to acknowledge the replication 
only spikes the latency. Also adding the fact that the Leader also has to communicate with  the database, 
databse process the request, process the response and send it back to the client


While the raft engine mostly priotizes Consistency, performance related things shouldn't be an 
afterthought, hence the hardset timeout. This is just the case for now, and will be increased later 
on. Preferrably via the config_toml file


If the node that receives this CommandRPC is not the Leader,  it rejects the command and 
tells the client to forward the request to the leader. This behavior is simply adhoc as 
the log-replication layer is still in development. The expected behavior is that the node 
forwards this command to the Leader. It creates the illusion to the client that it only 
talks to one server



Bug Documentation and Testing
---
Most of the tests are simulation tests where they are ran against an active cluster
to assert correct behaviour. This was preferred in favor of normal-unit tests as faults are 
easier to detect in an active cluster compared to testing it in isolation. The testing 
simulation can be configured via the toml file.

Another kind of test used is soak-testing  by running a cluster for over long hours. This 
can typically be found on the `profiling` branch as it's kept more up to date. Bugs 
are usually found on this branch. For example running a 13 node cluster over night got over 
690 go-routines. Another one which is partially documented is all nodes missing 4 minutes 
of logs, which is bizzare and could point to a myriad of different things.

Some of these can also be found in the commit logs under the  prefix of 
`profiling: ` or `wip: ` or `fix: ` or `testing: ` with reasonlably summarised explanations


An example is 
```
commit 24aa7e3e64cc706d0934aa0f1c2d9f4a391984a6 (code/profiling)
Author: persona-mp3 <randomnobscurebs@gmail.com>
Date:   Thu Jul 16 14:19:37 2026 +0100

    profiling: whole cluster was paused for 4 minutes
    Synopsis
    ----
    While reading the logs, I was able to trace how the last term, the 4th one
    was arrived at. It was gotten by 6 and it was taken from 3. Across all node logs
    there were missing logs from 5:50 to 5:54am. And only then did 6 realise that it
    had not recvd a heartbeat. So did the others, but since they had random timeouts
    6 won the election, and 2 had to step down right as it got into candidate state.
    
    Possible causes
    ---
    1. The laptop just slept
    2. The OS was overloaded or decided that our process had to be suspended for 4mins
    
    Comments
    ---
    This kind of thing further pushes for the following:
    1. making nodes running in isolation rather than on a single process
    2. using docker to run instances of them
    3. using different cloud instances

```

Bugs like these are usually documented, when it has caused enough pain in the `bugs` folder
along side stack traces for easy referencing. Alongside their fixes



In progress 
---
- [ ] Log replication across the cluster
- [ ] Integrate [jkvs](https://github.com/persona-mp3/jkvs) with fsm


Todos
---
- [ ] UI Control plane 


Done
---
- [X] Implement custom logger
- [X] Adding tests
- [X] Implementing simulation testing
- [X] Starting cluster from a config file,  `cluster_config.toml` with default number of nodes 3
- [X] Leader Election
    - [X] Refactor Follower 
    - [X] Refactor Leader
    - [X] Refactor Candidate


Contribute
---
Feel free to contribute
