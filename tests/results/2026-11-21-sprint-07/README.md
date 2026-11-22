# Actual Sprint 07 evidence

`final-build-matrix.log`: final Debug/Release/ASan+UBSan CTest output, each 1/1 PASS.
`restart-integration-final.log`: real broker restart/streak/delivery fixture.
`alerts.json`: consumed initial CPU/RAM alerts (initial development identity encoding).
`dlq.log` and `group-offsets.log`: invalid-version DLQ and committed input offset.
Earlier failed restart log is retained; final fixture observes new alerts after end offset.
`delivery-outage-final.log`: real broker alert transport fault, non-commit and identity-preserving retry.
Case inventory: ../../TEST_CASES.md (8 PASS, 1 NOT RUN).
