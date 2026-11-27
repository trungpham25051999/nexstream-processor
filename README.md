# nexstream-processor

[![CI](https://github.com/trungpham25051999/nexstream-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/trungpham25051999/nexstream-processor/actions/workflows/ci.yml) ![C++20](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus) ![Kafka](https://img.shields.io/badge/Kafka-consumer%20group-231F20?logo=apachekafka) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

C++20 Kafka telemetry analytics worker. Tested with librdkafka 2.0.2 and nlohmann JSON 3.11.2.

Build: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DRDKAFKA_ROOT=<prefix> -DJSON_INCLUDE_DIR=<include>`;
then `cmake --build build && ctest --test-dir build --output-on-failure`.
System installations also work. `NEXSTREAM_ENABLE_SANITIZERS=ON` enables ASan/UBSan.

Run: `./build/nexstream-processor localhost:19092 nexstream.processor.v1 90 90 3 /state/processor.checkpoint`.
Arguments are broker, group, CPU threshold, RAM threshold, consecutive sample count, diagnostic path.
Use a writable per-instance path. The diagnostic file is flock-owned, bounded to 512 KiB and **never
loaded for recovery**. Defaults: thresholds 90%, 3 consecutive samples; at most 16 assigned partitions,
128 hosts and 16 recent IDs per host/partition. Capacity exhaustion stops without committing input.

Consumes `telemetry.events.v1`; publishes deterministic CPU/RAM alerts to `telemetry.alerts.v1`.
Threshold comparison is >=; RAM percent = used/total*100. Streak resets below threshold;
each subsequent violating sample emits an alert. Recent duplicates and older timestamps do not advance
streaks. Recent dedup is bounded, not permanent. See contracts `docs/KAFKA-JSON-V1.md`.

Offsets are manual. Alert/DLQ delivery reports must succeed before the diagnostic write and offset
commit. Commit result events have a three-second application deadline; processing never advances
to the next input while commit is unresolved. Producer uses acks=all/idempotence, bounded
100-message/1 MiB queue and 5 s message timeout.
An unresolved delivery stops processing without committing input. No infinite application retry loop.
Consumer queue/fetch budgets are 1 MiB and transport response limit 2 MiB; parser rejects >64 KiB or
nesting beyond four levels. Allocation failure exits without classifying a valid record as malformed.
Malformed events are delivered as metadata-only DLQ records before commit; original payload remains
in source retention. Broker errors are polled without committing nonexistent work.

Each assignment reconstructs partition state from the Kafka committed prefix. Replay emits no output;
live records publish before diagnostic fsync/rename and confirmed offset commit. The offset metadata
binds the cursor to algorithm version and CPU/RAM/sample profile. Revocation clears prior ownership.
Empty assignments support more workers than partitions. Lost ownership/commit failures stop processing.
Crash after output but before commit can repeat alerts with unchanged IDs: this is at-least-once.

Recovery requires the full uncompacted, nontransactional source prefix from offset zero, no gaps and
stable host-key partitioning. It is bounded to 100000 offsets and 30 seconds per partition. Missing history,
profile mismatch or exhausted budget fails closed. A live worker may continue after old history expires,
but cannot safely restart/rebalance without retained history. This is an explicit production-scale limit.
See deploy ADR-017. Use a reviewed new group/replay or state migration for legacy cursors; never silently
reset offsets. Uniform configuration per group is an operator precondition, not cluster-wide consensus.

Assignment establishment, reconnect and stalled-partition progress have 30-second budgets (health
probes every five seconds); producer delivery timeout remains five seconds. Shutdown finishes/fails
the current bounded record and closes consumer ownership through RAII with a six-second close-queue
drain budget, then forces local destruction. Membership can persist until broker session expiry after
a forced close. All callbacks dispatch on the
worker polling thread; no raw connection/state pointer is retained by delivery reports.

Tests: `tests/processor_tests.cpp`, real broker `python3 tests/real_kafka_test.py` (S07 fixture).
`python3 tests/delivery_outage_test.py` provisions a separate real Redpanda and a loopback fault proxy,
then verifies unconfirmed alert delivery leaves the input pending and restart reuses the identity.
Actual results are under `tests/results/2026-11-21-sprint-07/`.
Hardening tests/evidence: `tests/hardening_{rebalance,memory,deadline}_test.py` and
`tests/results/2026-11-22-sprint-07-hardening/`. Rebalance/memory fixtures use dedicated
`nexstream-s07h-kafka` on loopback 29093 with two input partitions.

Development Compose also caps Processor memory at 256 MiB and pids at 64. SDK receive/fetch limits
are not a proof of bounded decompression for adversarial compressed batches; use a process/container
budget for native production execution too. The current fixture RSS test covers the recorded plain
malformed/oversized workload, not compression bombs. An OOM termination must leave input for replay.
