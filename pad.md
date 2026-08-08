
<!-- time=2026-08-08T21:30:39.231+01:00 level=INFO msg="we need to apply 1 logs before applying the current one: 1" -->
-- time=2026-08-08T21:30:39.231+01:00 level=INFO msg="we need to apply 4 logs before applying the current one: 2"
<!-- time=2026-08-08T21:30:39.231+01:00 level=INFO msg="we need to apply 1 logs before applying the current one: 0" -->
time=2026-08-08T21:30:39.231+01:00 level=INFO msg="we need to apply 3 logs before applying the current one: 3"
time=2026-08-08T21:30:39.231+01:00 level=INFO msg="we need to apply 5 logs before applying the current one: 5"
time=2026-08-08T21:30:39.232+01:00 level=INFO msg="we need to apply 5 logs before applying the current one: 4"
2026/08/08 21:30:39 INFO append new log to entry appended=true
2026/08/08 21:30:39 INFO append new log to entry appended=true
2026/08/08 21:30:39 INFO append new log to entry appended=true
2026/08/08 21:30:39 INFO append new log to entry appended=true
2026/08/08 21:30:39 INFO append new log to entry appended=true
2026/08/08 21:30:39 INFO append new log to entry appended=true

[21:32] darwin::jraft (feat/log-replication) | go run --race . --config cluster-config.toml
2026/08/08 21:32:13 running cluster in 'cluster' mode with total of 2 [localhost:5001 localhost:5002]
2026/08/08 21:32:13 pprof server running on: http://localhost:6061/debug/pprof/

<!-- time=2026-08-08T21:32:22.658+01:00 level=INFO msg="we need to apply 1 logs before applying the current one: 0" -->
<!-- time=2026-08-08T21:32:22.659+01:00 level=INFO msg="we need to apply 1 logs before applying the current one: 1" -->
-- time=2026-08-08T21:32:22.659+01:00 level=INFO msg="we need to apply 3 logs before applying the current one: 2"

time=2026-08-08T21:32:22.658+01:00 level=INFO msg="we need to apply 5 logs before applying the current one: 4"
time=2026-08-08T21:32:22.658+01:00 level=INFO msg="we need to apply 4 logs before applying the current one: 3"

