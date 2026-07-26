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
- Added single node cluster config for running nodes in isolation
- Added setup.sh and updated readme
- Refactored configuration layer
- Dynamic configuration and deployment
- Collected logs and generated config after running remotely

### 🐛 Bug Fixes

- Corrected mutex use in clearLeader
- Fixed packet framing on jkvs and jkvs.Conn.Read for Commit database
- Corrected branching
- Unhandled branch where logs and commitIndex are not up to date on handler appendRPC
- Fixed import
- Fixed locking order in raft.HasVoted and TODO docs
- Added cleanup for each worker when returning

### 💼 Other

- Scafolding State implementations
- Fixed config parsing and segfaults
- Mapping out how to impl log replication across the database
- Scafoldding for log-replication

### 🚜 Refactor

- Updated voteRPC handlers for followers

### 🎨 Styling

- Applied formatter

### 🧪 Testing

- Added AppendEntry tests for node and also wired  up server with raftlogger
- Fixed tests
- Added unit test for appendEntry handler
- Added new tests

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
- Added CHANGELOG.md using git-cliff from cargo
- Removed redundant
- Applied formatting
- Updated README
- Updated README with constraints
- Updated README with bugs and testing
- Updated README with installtion and running instructions
- Deleted prof file
- Updated .gitignore
- Added docs
- Corrected toml field
- Updated README
- Added time-logging for scp and ssh commands and included logging and config samples for test nodes
- Updated readme
- Added sample deploy.toml
- Updated readme
- Added database documentation, change network io to ReadFull  and corrected logging
- Applied changes
- Added escape codes
- Updated README
- Made network channel buffered
- Refactored handleAppendEntry for follower and added stub for logMatching and verifying leader
- Removed logs
- Removed debug print
- Added formatting
- Added test workflow
- *(fix)* Fixed test output directory and test flags
- Removed dead comments and tested changes across runs
- Removed dead comments and debug printing
- *(ci)* Added race detector to test workflow
- Added formatting and more pseudocode to represent appending logs to local logs
- Moved worker replication to attemptSend function
- Resolved comments on PR
- Repository cleanup
