# Sprint 07 test inventory

| ID | Case | Final status | Evidence |
|---|---|---|---|
| P01 | Consecutive CPU/RAM threshold and reset | PASS | final-build-matrix.log |
| P02 | JSON version/type/semantic rejection | PASS | final-build-matrix.log |
| P03 | Recent duplicate suppression and host bound | PASS | final-build-matrix.log |
| P04 | Checkpoint preserves streak | PASS | final-build-matrix.log |
| P05 | Real Kafka alert delivery across restart | PASS | restart-integration-final.log |
| P06 | Invalid version DLQ and subsequent input commit | PASS | dlq.log, group-offsets.log |
| P07 | Broker idle disconnect/restart, process survives | PASS | recorded tool output; final report |
| P08 | Transport outage during in-flight alert delivery | PASS | delivery-outage-final.log, real broker and loopback fault proxy |
| P09 | Multi-worker rebalance/checkpoint transfer | NOT RUN | local checkpoint limitation |

8 PASS, 0 FAIL, 0 BLOCKED, 1 NOT RUN. Build matrix covers unit executable, not all real integrations under ASan.

## Sprint 07 hardening (current acceptance catalog)

Original results above are historical. Multi-owner coverage formerly missing in P09 is now exercised
by H-P02/H-P03/H-P14; no historical log is retroactively relabeled.

| ID | Case | Status | Actual evidence in results/2026-11-22-sprint-07-hardening/ |
|---|---|---|---|
| H-P01 | Partition committed-prefix replay suppresses old outputs; gap and replay-count guards | PASS | build-matrix-final.log |
| H-P02 | Two instances, same group, two partitions; reassignment preserves streak | PASS | rebalance-final.log |
| H-P03 | SIGKILL owner, survivor reconstructs and continues | PASS | rebalance-final.log |
| H-P04 | Output success / diagnostic checkpoint failure: no commit, stable duplicate IDs | PASS | rebalance-final.log |
| H-P05 | Existing group cursor with changed rule profile fails closed | PASS | rebalance-final.log |
| H-P06 | In-flight alert transport failure and original-identity retry | PASS | alert-outage-and-retention.log |
| H-P07 | DLQ failure, oversized source, broker + consumer restart retry same offset | PASS | dlq-outage.log, asan-integration-final.log |
| H-P08 | 100 nested malformed + 20 oversized records; 120 DLQ, observed RSS bound | PASS | memory-final.log, memory.json |
| H-P09 | Real broker restart and live consumer recovery | PASS | memory-final.log |
| H-P10 | Bounded graceful shutdown | PASS | memory-final.log |
| H-P11 | Missing retained source prefix rejects recovery | PASS | alert-outage-and-retention.log, asan-integration-final.log |
| H-P12 | Debug/Release/ASan+UBSan unit suites | PASS | build-matrix-final.log |
| H-P13 | Delivery callback ownership and cleanup under real ASan fault/restart | PASS | asan-integration-final.log |
| H-P14 | Third instance on two partitions: idle member healthy, offsets advance | PASS | rebalance-final.log |
| H-P15 | Strict numeric config + bounded initial assignment/reconnect failure | PASS | deadline-final.log |
| H-P16 | Confirmed output + failed OffsetCommit: deadline, no early cursor, stable duplicates | PASS | commit-outage.log |

Hardening: 16 PASS, 0 FAIL, 0 BLOCKED, 0 NOT RUN. Cases count behavior, not reruns/assertions.
