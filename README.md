FSM - Raft Engine for JKVS 
--
This implementation follows the paper [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
called Raft.  The aim of this project is to build a distributed database. The underlying database is a custom
database built in Java. It was originally inspired by the PingCaps TiKV talent plan, but I adopted other methods 
along the way instead of following the full spec.  For more information about the database, jkvs,
visit [JKVS Database](https://github.com/persona-mp3/jkvs.git)


FSM is the replication engine that powers JKVS, implementing the [Raft Consensus Algorithm](https://raft.github.io/raft.pdf).
It handles leader election, log replication and failure recovery, so the cluster keeps serving reads 
and writes provided majority of the nodes are healthy. 

FSM follows the original raft paper. The underlying database is inspired by PingCAP's TiKV Talent plan, 
though implementation details have diverged into it's own architecture either for performance or tailored needs


Running FSM
---
#### You need
- [Apache Maven](https://maven.apache.org/) 
- Java at least version 21 (virtual threads)
- [Go](https://go.dev/doc/install) at least version 1.26.3

Like most databases, FSM follows a server-client model, where many clients connect to (potentially 
distributed) server. The server in this case is JKVS. By running the jar file `target/jkvs/jkvs-server.jar`. The database 
must be started before FSM can work. To automate this, simply run the `setup.sh` script.

#### Run the database
```sh
~/jkvs/jkvs $ mvn package 
~/jkvs/jkvs $ java -jar ./target/jkvs-server
```

To run the raft layer, you need to have Go installed to build the binary or simply run it
#### Run the program
```go
go run .
```

#### To execute it as a standalone binary
```go
go build -o fsm . 
./fsm
```

FSM will connect to the jkvs server before starting up,  otherwise it fails to run
FSM can be confgiured to run in a set number of ways including deployment via ssh. You can read more about that on [docs/configuration](./docs/configuration.md)


Interacting with FSM
---
At the moment, there is only one way to interact with FSM and that is through a simulation repl
```go
go run simulation/repl.go
```

It allows to send commands JKVS supports, `get`, `set` and `rm`. Since this is still in 
development, it is not polished. For testing, we use the the `simulation/client-request.go` instead


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
[time.ms] (nodeId:state:term) Information
```

An example logs as such
```
21:48:40.123340 (1:node:0) successfully connected to database
# This node with an id of 1, the current point of execution is within the node component of the application, and is in it's 0th term
```
``

```
21:48:41.543572 (1:leader:2) leader state transitioned successfully diagnostics: { id: 1, term: 2, state: Leader, votedFor|leader: , logs:  }
# This node with an id of 1, is the leader for the current term and it just started
```


Testing 
---
Testing a running cluster is done via the [test.toml](./test.toml) config. This is more emphasized over
unit tests to help tweak behaviours and match against behaviours that are expected in a cluster, as it 
helps with dynamic configuration, leans towards property-based testing.  The development for this 
is still ongoing, so it's not yet polished. To run a test, the test.toml must be present and a cluster must 
be active

```go
go run simulation/lead.go
```


Architecture
---
See [architecture](./docs/architecture.md)


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



Bug Documentation 
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
- [X] Starting cluster from a config file,  `cluster-config.toml` with default number of nodes 3
- [X] Leader Election
    - [X] Refactor Follower 
    - [X] Refactor Leader
    - [X] Refactor Candidate


Contribute
---
Feel free to contribute

Liscense
---
MIT
