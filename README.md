FSM - Raft Engine for JKVS 
--
FSM is the replication engine that powers [JKVS](https://github.com/persona-mp3/jkvs.git), implementing the [Raft Consensus Algorithm](https://raft.github.io/raft.pdf).
It handles leader election, log replication, and failure recovery, so the cluster keeps serving reads
and writes provided a majority of the nodes are healthy. The underlying database is a custom key-value store
built in Java, originally inspired by PingCAP's TiKV talent plan, though implementation details have
diverged into their own architecture for performance and tailored needs.


Running FSM
---
### Prerequisites
- [Apache Maven](https://maven.apache.org/) 
- Java at least version 21 (virtual threads)
- [Go](https://go.dev/doc/install) at least version 1.26.3

#### Using Docker
The Dockerfile is a multi-stage build that compiles both FSM and the JKVS database into a single image, so no local Java or Go installation is required.

```sh
docker build -t fsm .
docker run -p 5001:5001 -p 5002:5002 -p 6061:6061 -p 9090:9090 fsm
```

The container starts JKVS automatically before FSM, so there is no need to run the database separately. Ports `5001` and `5002` are the peer ports defined in `cluster-config.toml`, `6061` is the pprof endpoint, and `9090` is the JKVS database port.

Like most databases, FSM follows a server-client model, where many clients connect to a (potentially
distributed) server. The server in this case is JKVS, started by running the jar file `target/jkvs/jkvs-server.jar`.
The database must be started before FSM can work. To automate this, simply run the `setup.sh` script.

#### Run the database
```sh
~/jkvs/jkvs $ mvn package 
~/jkvs/jkvs $ java -jar ./target/jkvs-server
```

To run the Raft layer, you need Go installed — either run it directly or build the binary first.

#### Run the program
```go
go run .
```

#### To execute it as a standalone binary
```go
go build -o fsm . 
./fsm
```

FSM will connect to the JKVS server before starting up, otherwise it fails to run.
FSM can be configured to run in a set number of ways including deployment via SSH. You can read more about that in [docs/configuration](./docs/configuration.md).


Interacting with FSM
---
At the moment, there is only one way to interact with FSM, and that is through a REPL.

```go
go run repl/repl.go
```

It allows you to send commands JKVS supports, `get`, `set` and `rm`. Since this is still in
development, it is not polished. For testing, we use the `simulation/client-request.go` instead.


Observability and Tooling
---
### pprof integration
You can monitor the cluster at [http://localhost:6061/debug/pprof/](http://localhost:6061/debug/pprof/) as it 
uses Go's built-in pprof library

### Custom loggers
The logger used at this current state of the application serves for easy debugging. This is 
in no way designed to be performant and will be ripped out later on in favor of structured logging.
Depending on the number of nodes running in the cluster, the same number of log files for each node 
will be created in the current directory. If you have 5 nodes running, their respective log files will be
named with the prefix `log-file-node-id`.

To be able to understand the logs,
```
[time.ms] (nodeId:state:term) Information
```

An example looks like this
```
21:48:40.123340 (1:node:0) successfully connected to database
# This node with an id of 1, the current point of execution is within the node component of the application, and is in its 0th term
```

```
21:48:41.543572 (1:leader:2) leader state transitioned successfully diagnostics: { id: 1, term: 2, state: Leader, votedFor|leader: , logs:  }
# This node with an id of 1, is the leader for the current term and it just started
```


Testing 
---
Testing a running cluster is done via the [test.toml](./test.toml) config. This is preferred over
unit tests to help tweak behaviours and match against behaviours expected in a cluster, as it
helps with dynamic configuration and leans towards property-based testing. The development for this
is still ongoing, so it's not yet polished. To run a test, the `test.toml` must be present and a cluster must
be active.

```go
go run simulation/lead.go
```


Architecture
---
See [architecture](./docs/architecture.md)


Constraints
---
- **Latency**: Sending a command to a majority of a cluster takes a lot of time and causes noticeable
  latency from the client's perspective. Waiting for a quorum of Followers to acknowledge the replication
  only spikes this further. Additionally, the Leader has to communicate with the database, which processes
  the request and sends the response back to the client.

- **Timeout**: While the Raft engine mostly prioritizes consistency, performance-related things shouldn't be an
  afterthought, hence the hard-set timeout. This will be made configurable later on, preferably via the
  `cluster-config.toml` file.

- **Non-leader nodes**: If the node that receives a CommandRPC is not the Leader, it rejects the command and
  tells the client to forward the request to the leader. This behavior is simply ad hoc as the log-replication
  layer is still in development. The expected behavior is that the node forwards the command to the Leader,
  creating the illusion for the client that it only talks to one server.



Bug Documentation 
---
Most of the tests are simulation tests where they are run against an active cluster
to assert correct behaviour. This was preferred in favor of unit tests as faults are
easier to detect in an active cluster compared to testing it in isolation. The testing
simulation can be configured via the toml file.

Another kind of test used is soak-testing by running a cluster for extended periods. This
can typically be found on the `profiling` branch as it's kept more up to date. Bugs
are usually found on this branch. For example, running a 13-node cluster overnight leaked over
690 goroutines. Another one which is partially documented is all nodes missing 4 minutes
of logs, which is bizarre and could point to a myriad of different things.

Some of these can also be found in the commit logs under the prefix of
`profiling: ` or `wip: ` or `fix: ` or `testing: ` with reasonably summarised explanations.


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

Bugs like these are usually documented once they have caused enough pain, in the `bugs` folder
alongside stack traces and their fixes for easy referencing.



In progress 
---
- [ ] Log matching property


Todos
---
- [ ] UI Control plane 


Done
---
- [X] Implement custom logger
- [X] Adding tests
- [X] Implementing simulation testing
- [X] Starting cluster from a config file, `cluster-config.toml` with default number of nodes 3
- [X] Leader Election
    - [X] Refactor Follower 
    - [X] Refactor Leader
    - [X] Refactor Candidate
- [X] Log replication across the cluster
- [X] Integrate [jkvs](https://github.com/persona-mp3/jkvs) with fsm


Contribute
---
Feel free to contribute

License
---
MIT
