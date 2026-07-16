# Bug Report: RPC connection & goroutine leak on candidate election

For the Vote Protocol, the function `n.runCandidate` had an unhandled edge case, which caused a connection and goroutine leak. The unhandled edge case was that on transitioning into `Candidate` state, the node dialed a fresh RPC connection to every peer to send `RequestVote` calls, but never closed the *previous* connections held for that peer, and never signaled the outgoing `runFollower` goroutine to exit. Each connection and goroutine was simply abandoned, blocked forever waiting on a read that would never come. On a 3 or 5 node cluster, this isn't noticeable, but with 13 nodes running long enough to see a handful of elections, it didn't justify seeing the goroutine count triple, then climb to 289, across three profiling runs.

The bug was identified via successive goroutine profiles across three separate runs, cross-checked with `lsof`. The stack traces are shown in 
[goroutine-profiles-run-1](./goroutine-profiles.txt).

[goroutine-profiles-run-1](rpc-conn-1.txt)
[goroutine-profiles-run-2](rpc-conn-2.txt)
[goroutine-profiles-run-3](rpc-conn-3.txt)

## How it was identified

Across three profiles of the same topology (13 nodes, 12 peers each), the RPC connection counts grew where they should have stayed flat:

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `net/rpc.(*Client).input` | 12 | 36 | 108 |
| `net/rpc.(*Server).ServeConn` | 12 | 36 | 108 |

12 matches the expected steady state (one connection per peer). The growth to 36, then 108, tracked with elections occurring in the background — each blocked in `internal/poll.runtime_pollWait`, i.e. genuinely idle, not stuck processing anything:

```
108 @ 0x4c852e 0x487d77 0x4c76e5 0x575f71 0x577af9 0x577acf 0x6536cb 0x667f4d 0x5cbf18 0x535411 0x5de545 0x5de4d8 0x5ebed8 0x5ec57e 0x5ecc45 0x5ec90e 0x96aa0f 0x9693d2 0x4d01c1
#	0x4c76e4	internal/poll.runtime_pollWait+0x84		.../runtime/netpoll.go:351
#	0x96aa0e	net/rpc.(*gobClientCodec).ReadResponseHeader+0x4e	.../net/rpc/client.go:228
#	0x9693d1	net/rpc.(*Client).input+0x111				.../net/rpc/client.go:109
```

Alongside this, `runFollower` goroutines increasingly appeared **without** their expected `Node.Run.func2` parent frame — a second, untracked spawn path, while the original supervised goroutine for that node stayed blocked and un-exited:

```
8 @ 0x4c852e 0x4a3cf7 0x9f7c4d 0x4d01c1
#	0x9f7c4c	main.(*Node).runFollower+0x34c	.../follower.go:26
```
(no `Node.Run.func2` frame — orphaned)

`lsof -i :4002 -T qs` confirmed these were real, fully `ESTABLISHED` sockets on both ends — not profiler artifacts — with `QR=0` on every one, i.e. idle and abandoned rather than backpressured:

```
COMMAND   PID   USER  FD   TYPE DEVICE SIZE/OFF NODE NAME
fsm     82568 house  36u  IPv4 247183      0t0  TCP localhost:pxc-spvr-ft (LISTEN)
fsm     82568 house  69u  IPv4 245336      0t0  TCP localhost:36688->localhost:pxc-spvr-ft (ESTABLISHED)
fsm     82568 house  72u  IPv4 247206      0t0  TCP localhost:pxc-spvr-ft->localhost:36688 (ESTABLISHED)
fsm     82568 house  99u  IPv4 249045      0t0  TCP localhost:36698->localhost:pxc-spvr-ft (ESTABLISHED)
fsm     82568 house 100u  IPv4 250981      0t0  TCP localhost:pxc-spvr-ft->localhost:36698 (ESTABLISHED)
fsm     82568 house 111u  IPv4 253027      0t0  TCP localhost:mapx->localhost:pxc-spvr-ft (ESTABLISHED)
fsm     82568 house 112u  IPv4 241141      0t0  TCP localhost:pxc-spvr-ft->localhost:mapx (ESTABLISHED)
fsm     82568 house 115u  IPv4 236324      0t0  TCP localhost:36710->localhost:pxc-spvr-ft (ESTABLISHED)
fsm     82568 house 117u  IPv4 240399      0t0  TCP localhost:pxc-spvr-ft->localhost:36710 (ESTABLISHED)
fsm     82568 house 139u  IPv4 253035      0t0  TCP localhost:36724->localhost:pxc-spvr-ft (ESTABLISHED)
fsm     82568 house 142u  IPv4 245357      0t0  TCP localhost:pxc-spvr-ft->localhost:36724 (ESTABLISHED)
fsm     82568 house 161u  IPv4 252059      0t0  TCP localhost:pxc-spvr-ft->localhost:36740 (ESTABLISHED)
fsm     82568 house 162u  IPv4 238239      0t0  TCP localhost:36740->localhost:pxc-spvr-ft (ESTABLISHED)
```

Ports 4003 and 4004 (the other two nodes' RPC listeners) showed the identical pattern — 6 established connection-pairs each, all loopback, all belonging to the same process — confirming this was a full mesh of dangling self-connections rather than an issue isolated to a single node or port.

## The Fix

Resource cleanup was added on the path where a node fails an election, closing out its held peer connections instead of leaving them open indefinitely:

```go
func (n *Node) closeConnections() {
	peers := n.getRPCPeers()
	for _, p := range peers {
		if err := p.Close(); err != nil {
			n.log.Println(err.Error())
		}
	}

	n.addRPCPeer([]*Peer{}...)
}
// this is called on paths where this current node fails an election
```

Re-profiled across repeated forced elections; RPC client/server connection counts and goroutine counts now return to the expected per-peer baseline (12/12) after each election instead of accumulating.


