# Actual hardening evidence

Required catalog H-P01..H-P16: 16 PASS. Counts exclude repeat executions/configurations.
`build-matrix-final.log`: final Debug/Release/Debug+ASan/UBSan unit suites.
`rebalance-final.log`, `release-rebalance-final.log`: real two active owners, third idle member,
SIGKILL takeover, profile mismatch and stable duplicate IDs after confirmed output/checkpoint failure.
`alert-outage-and-retention.log`, `dlq-outage.log`, `asan-integration-final.log`: real broker fault proxy,
consumer/broker restart, pending input and missing-prefix fail-closed behavior.
`memory-final.log` / `memory.json`: final malformed/oversized workload, observed RSS, broker restart/shutdown.
`commit-outage.log`, `asan-commit-outage.log`: dropped OffsetCommit after output confirmation and
diagnostic write, bounded exit, unchanged source retry and stable duplicate alert IDs.
`deadline-final.log`: strict config parsing and actual 30.134-second unavailable initial-assignment exit.
`rebalance.log` preserves the first failure (control characters in metadata); final fingerprint is hex.
See canonical deploy hardening report and ADR-017 for operational limits and actual command families.
