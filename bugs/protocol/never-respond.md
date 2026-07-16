For the Vote Protocol, the function `n.handleVoteRequest` had an unhandlede edgecase, which caused a memory leak 
and goroutine explosion. The unhandled edge case was that if the node received an VoteRPC from  another Candidate node
and they both had the same term, it would simply just not send a response back to the client node. And thus
the node going on about it's way of life. On a 3 or 5 node clusters, this won't be noticeable, but with 
13 nodes, it didn't justify seeing 689 goroutines print to my terminal in the morning

The bug was identified via a fresh run using pprof. The stack trace is shown in [never-resposnd](./never-respond.txt)

How it was identified
---
In the goroutine stack trace, lines 43 and 44 always appeared in previous runs since the
cluster was running 13 nodes. It was stuck inside the `n.collectVote` routine function which makes 
an rpc call to the client it has. specifically on line 171 waiting for a response

```go
	reply := &VoteReply{}
  // blocked here indefinitely
	if err := peer.rpcConn.Call("Server.VoteRequestRPC", req, reply); err != nil {
		logger.Println("could not dial rpc client:", peer.addr, err)
		return
	}
```

On the callees' perspective
---
The handler simply returned from the function without sending a response back to the client via the server

The Fix
---
```go
	replyCh <- RPCReply{
		kind: Vote,
		payload: &VoteReply{
			Id:       n.id,
			VotedFor: false,
			Term:     req.Term,
			Message:  "we both have the same term and I already have a leader, why did this happen?",
		},
	}
```


22 @ 0x118108e 0x110fc4f 0x110f7d2 0x16a5d5e 0x16a5b7b 0x16a550b 0x1188fa1
#	0x16a5d5d	net/rpc.(*Client).Call+0x1fd		/Users/eghosa.aiwuyo/homebrew/Cellar/go/1.26.4/libexec/src/net/rpc/client.go:321
#	0x16a5b7a	main.(*Node).collectVote+0x1a		/Users/eghosa.aiwuyo/dev/fsm/candidate.go:170
#	0x16a550a	main.(*Node).runCandidate.func2+0xea	/Users/eghosa.aiwuyo/dev/fsm/candidate.go:58


Commit logs
---
```
commit 3ab844f140a59332c142f7afa2adc5ba64995280
Author: persona-mp3 <randomnobscurebs@gmail.com>
Date:   Thu Jul 16 08:28:26 2026 +0100

    testing: 13 node cluster with 200ms Heartbeat
    Goroutines: 689
    Terms: 4
    Eelection timeouts: 500 - 1200ms
    
    Note: There seems to have been a network drop, because node-6 did not
    receive any heartbeat from the leader for a whole 4minutes

commit fda57bff000b1e64374408d4bad3d434a62c2eee
Author: persona-mp3 <randomnobscurebs@gmail.com>
Date:   Thu Jul 16 05:29:50 2026 +0100

    chore: profiling cluster with 400ms hb and 900-1800ms for election timeouts.
    Cluster size: 3 
    Heartbeat: 400ms
    Election timeouts: 900 - 1800ms
    
    Notes:
    At some point, there was another election that took place where the first leader dropped
    But looking at pprof goroutine, there was a stack trace of candidate runs still there
    which is suspicious, taking the fact that the cluster is actually healthy

```
