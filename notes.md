leader
---
The leader accepts logEntries from clients, replicates them 
on other servers, and tells the servers when it's safe to apply 
the log entry to their state machine

problems
---
1. leader election
2. log replication
3. saftey

