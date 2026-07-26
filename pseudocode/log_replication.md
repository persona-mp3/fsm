Log Replication in a Raft Cluster
---

When a Leader receives a client command, it first creates an entry for this, and 
appends it to it's logs.
```go
type Operation int

const (
    Get Operation = iota
    Set 
    Remove
)

type JKVSCommand struct {
  Key       string
  Value     string
  Operation Operation 
}

type Entry struct {
  // position in the leaders logs
  id   uint

  // term when the Leader recvd the command
  Term uint64

  // request issued from the client. In this case, it will be a JKVS command
  Command JKVSCommand
}


type Raft struct {
    currentLogs []*Entry

    // this will be the highest log index that the leader has told jkvs to apply
    commited uint
    
    // ...other fields
}
```

After the leader receives the request from the client, it then replicates the Entry 
among a majority of the cluster, by sending   AppendEntryRPC's to other nodes in the 
cluster. After that, it executes the command locally and sends the response back to 
the client. And then, it commits that entry to this logs. IE I have done this action, 
others should also do it, it is safe and final. 
It then communicates this committed entry via AppendEntryRPC's to other Followers in the 
cluster by using the entry's own log index. Which signals to Followers that they can 
now commit/execute the command locally. 
(
  Or rather, I think the leader sends the result it executed? 
  because it's possible that both nodes with the same database, executing 
  commands might yield different results.
)

So the leader always has to keep track of it's most committed entry.
We could expand that to resemble 
```go
type CommitedEntry struct {
  index uint
  term  uint
  // not sure if we also want the Followers to sync to the Leader's own time of execution...
  timestamp time.Duration
}
```
But since each Follower also tracks their most recent commitedIndex, they will also 
need to check it against their own currentTerm hence the previous expansion term


Then we can modify the AppendEntryRequest to look something like this:
```go
type AppendEntryRequest struct {
  From  uint
  Term  uint
  Apply CommitedEntry

  New   Entry
  // the follower can check if it has an entry that matches this. if it doesn't it
  // it rejects it
  PreviousEntryIndex uint
}
```
and then AppendEntryReply
```go
type AppendEntryReply struct {
  From         uint
  Term         uint
  // if the follower applied the commitedEntry in the request paylaod
  // the follower might refuse to apply it i
  Applied      bool
  // returns it's latest commitedEntry
  Commited     CommitedEntry
  Acknowledged bool
}
```

