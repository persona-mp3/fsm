## [unreleased]

### 🚀 Features

- Added state transition logic between Follower, Candidate and Leader RaftStates
- Refactored leader
- Refactored follower and created a stub for Candidate state
- Added custom rlogger
- Scafolded sending heartbeat loop for leader
- Merged reafactor branch into main
- Added rpc scafolding for client command
- Integrated jkvs locally
- Working on log replication
- Added log deduplication

### 🐛 Bug Fixes

- Corrected mutex use in clearLeader

### 💼 Other

- Scafolding State implementations
- Fixed config parsing and segfaults
- Mapping out how to impl log replication across the database

### 🧪 Testing

- Added AppendEntry tests for node and also wired  up server with raftlogger
- Fixed tests

### ⚙️ Miscellaneous Tasks

- Corrected defer and RWUnlock statements in states.go
- Updated and corrected docs for raft-core refactor
- Added documentation for runLeader
- Corrected and removed typo from docs
- Added comments on runFollower and removed redundant comments
- Removed redundant comments and files
- Added random condition to determine Leader transition from Candidate state
- Added signal handling in simulation/single-leader.go and return for leader state when sendingon transition
- Added workflow
- Change ci to use golangci.yml instead of args. outdated version
- Removed linter
- Increased election timeouts and metric logging of number of goroutines in main.main
- Updated readme
- Added logs and updated readme
- Addressed review and removed log files from repo
- Updated README
- Removed log file and empty file
- Removed devtools no longer needed as it's replaced with simulation
- Updated README
- Remove redundant comments
- Removed redundant comments and files
- Removed yaml
- Removed redundant comments
- Removed workflow
