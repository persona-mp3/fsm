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

