38e5655 Merge pull request #15 from persona-mp3/feat/log-replication
79f4529 fix: added cleanup for each worker when returning
6d0d9cd feat: refactored leader function to n.StartLeader Changes --- Created workers seperately for sending heartbeats and replicating new commands Increased buffers for network channel to default to 100 TODO --- Introduce property testing for node behaviour
1835836 impl: scafoldding for log-replication
f7e9703 Merge pull request #14 from persona-mp3/testing
ebd6588 test: added new tests
b658ce8 Merge pull request #13 from persona-mp3/dependabot/github_actions/action-updates-eb0635fa19
435d329 Merge branch 'main' of https://codeberg.org/ddanielaiwuyo/raft
88e842e ci(fix): fixed test output directory and test flags
d027909 ci: added test workflow
02e993f style: applied formatter
376569a Merge pull request 'new-leader-election' (#2) from new-leader-election into main
186ac97 Merge pull request 'fix-leader-election' (#1) from fix-leader-election into main
4f7e49b Merge pull request #12 from persona-mp3/fix/leader-election
f299914 fix: fixed locking order in raft.HasVoted and TODO docs
2dc992a fix: fixed import
3fcb00f Revise testing section in README
27b4658 test: added unit test for appendEntry handler
aef54e3 fix: unhandled branch where logs and commitIndex are not up to date on handler appendRPC
26833f9 Revise testing section in README
da81c54 refactor: updated voteRPC handlers for followers
ca0dc6a fix: corrected branching
b4cde71 fix: refactored follower network handler for AppendEntryRPC and slow replacement with slogger
3e8b4b7 Merge pull request #10 from persona-mp3/feat/configuration
4bda991 Fix comment formatting in test.toml
0015c25 feat: added test.toml for configuring test simulation. simulation/lead replaces single-leader.go
7bdaee3 updated gitignore
18ce21f feat: collected logs and generated config after running remotely
3ff4b46 feat: dynamic configuration and deployment
7d489e7 Merge pull request #9 from persona-mp3/config-refactorr
1dc12b0 feat: refactored configuration layer
fe5bba1 feat: adding deployment tooling dock  to deploy to remote servers Cluster configuation has changed and will now be more precise - Implemented new mode, Single for running a single node on a machine
59fd839 Update README.md
424ec43 Merge pull request #8 from persona-mp3/bug/protocol
eefdaec Delete findings.txt
930d31e fix: fixed goroutine explostion When a node turns into a candidate, rpc connections are not closed, even though it becomes a follower
08434c1 info: profiling and unhandled request path for VoteRPC Synopsis --- Apparently after profiling the custom logger uses a lot of memory, and because of that, alot of garbage collection happens. Due to this reason, a gc happening mid log can cause a gc spike and halt the node itself. The collection duration is enough to miss sending heartbeats to followers
24aa7e3 profiling: whole cluster was paused for 4 minutes Synopsis ---- While reading the logs, I was able to trace how the last term, the 4th one was arrived at. It was gotten by 6 and it was taken from 3. Across all node logs there were missing logs from 5:50 to 5:54am. And only then did 6 realise that it had not recvd a heartbeat. So did the others, but since they had random timeouts 6 won the election, and 2 had to step down right as it got into candidate state.
3ab844f testing: 13 node cluster with 200ms Heartbeat Goroutines: 689 Terms: 4 Eelection timeouts: 500 - 1200ms
117cb18 fix: fixed packet framing on jkvs and jkvs.Conn.Read for Commit database
ecb93a7 wip: log replication: client commands are executed on the database This current or stage is very troublesome. I'm hungry so I need to stop here Key features: 1. Log replication: The leader now interacts with the database with client commands, and sends the response as is to the client. We still have the framing logic mentioned in 08e2111 but I've decided to side step that for now
b279c0a feat: added setup.sh and updated readme
08e211e feat: raft layer making a commit on the jkvs database incoming client commands are now applied to jkvs, but at the moment, the result isn't sent to the client, just a stub. We'd need to apply the packet framing for correctly reading responses from jkvs which might need to revisiting the jkvs implementation - Changed Operation type to match the database' operations - Used Commit instead of Send on database interface - Assigned each peer a dropChannel for sending new client commands to the workers which removes the possibility of a worker missing a log entry
a1e4d12 feat: added single node cluster config for running nodes in isolation
5cf0439 Merge pull request #7 from persona-mp3/integrate/jkvs
2350869 fix: fixed data-race when node is dropping from leader to follower on Vote route fixed config for simulationn/single-leader by using defaultConfig instead of empty path
d93c025 feat: added log deduplication
468b1a0 feat: working on log replication
c2b19f8 feat: integrated jkvs locally
2cc1f8d feat: added rpc scafolding for client command
2d700e7 research: mapping out how to impl log replication across the database
c70119f Fix error handling in RPC dialing
72bf500 Revise README for clarity and corrections
ee8aa8b Update README.md
451a11c Merge pull request #5 from ddanielaiwuyo/patch-1
c550db1 Fix typos in profiling and monitoring section
9515009 feat: merged reafactor branch into main
b7849d0 fix: corrected mutex use in clearLeader
78280bc feat: leader election complete accross all states others: abstracted way rpcConns using Peers
4068c5b removed tests
49bc67e bug: fixed config parsing and segfaults
f363886 feat: fixed goroutine cleanup in runCandidate() diagnosis: on exit the runCandidate() routine had a timer bug where the timer was drained after it had already stopped
3697907 added profiler
3f2581c feat: scafolded sending heartbeat loop for leader
d90721e bug: fixed data race in test when reading current state of the node and configured yamlci to run tests with race-detector and json structured logging
bdb50f7 test: fixed tests
fc5a230 added todos
8f93fbc test: added AppendEntry tests for node and also wired  up server with raftlogger
188820f feat: AppendEntryRPC network configuratoin and Follower mode state 	1. The AppendEntryRPC has been created and only the Follower 	   state supports it fully. 	2. When a Follower receives an AppendEntryRequest from a higher 	   term, it stores the current leader of the term and validates 	   incoming rpcs against it 	3. Fixed bug in cluster.go when by correctly adding spawned 	   Nodes to the WaitGroup monitor
8683fbc feat: added custom rlogger
d307ffa desing: scafolding State implementations
43fd8d7 rewriting application. scafolded apis and structures
babcb39 Merge pull request #2 from persona-mp3/fix
1fd20a9 feat: refactored follower and created a stub for Candidate state
2cc0c6c feat: refactored leader
ce348b2 feat: Rewrote raft core Changes: 1. Rather than having an intermediary between the networkRPC sending them to each active state, the state itself owns the node. This makes it easier for understanding the flow of the program and removed alot of code. 2. State transition garuantees that the active state must stop. Now that each state can directly access the network, they can communicate with the mainLoop to to transition and at that point stops via a blocking channel. While the devtools has been tested to send requests in nanoseconds, there have been no stall to the single-node instance. 3. Each state has it's own internal operations clear and laidout
3e1c882 we did it guys: after two different iterations again, I decided to have a different frame of thought, which is also similar to my previous escapades. This current one has each `State` as the Node itself. For example, if the node goes into a Follower state, instead of an intermediary layer relaying network rpcs to it's own self contained mode, it owns the data types itself. So Candiate can directly recv all incoming rpcs from the server, same as Leader and same as Follower. With this new architecture, the intermediary layer is completely removed with a single flow loop. Each state is cancled by this loop, that will be called `stateManager` via a context. This context is stored in the Raft struct itself and is recreated on each new cancel. The stateManager knows how and when to transition when each state running in a routine, sends a new RaftState to run via the transitionCh, stored on the Raft struct. The node no longer stalls even though the devtools client sends rpcs every nano second, none of the channels are buffered which where on purpose to also test this out. Although extreme edgecases haven't be tested, this is working
2fa37ee bug: when in the follower mode, two things seem to be happening: 1. Some sort of delay in the program. While I haven't been able to pinpoint it yet, my best bet is it's from the raft.Run() loop and from the server, so those sections might need refactoring
3eb1c4b Merge pull request #1 from persona-mp3/leader-election
737b7c3 feat: Added state transition logic between Follower, Candidate and Leader RaftStates
b2af1dd removed redundant code
e4383ce got server and rpc setup working
7a4b773 added notes
68272ab added raft paper
c820b83 initial commit
