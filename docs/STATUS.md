# Current status — Sprint 07 hardening

Partition-scoped committed-prefix recovery, rebalance, takeover, offset profile binding and bounded
failure handling are verified against real Kafka-compatible Redpanda. 16 hardening cases PASS.
Debug/Release/ASan+UBSan units and real ASan fault integration passed.

Open P1 operational limit: recovery requires source offset zero through the committed cursor, at most
100000 prefix offsets, and 30 seconds per partition. Production-scale durable snapshots/changelog
remain a review item; recovery fails closed if prerequisites are absent. See deploy ADR-017 and
SPRINT-07-HARDENING-REPORT.md. No exactly-once claim or Sprint 08 implementation.
