# nexstream-processor — processing guide

> Sprint 07 local implementation executed; see README and tests/results for limits/evidence.

## Implemented hardening workflow

Assign partition -> validate committed cursor/profile and retained prefix -> replay committed history
without output -> evaluate live record -> confirm alert/DLQ deliveries -> diagnostic fsync -> await
next-offset commit on an owned SDK result queue (three-second deadline). Revocation clears ownership;
a new owner reconstructs independently. Close callbacks drain for six seconds before local teardown.
All state and SDK callbacks dispatch on the worker polling thread; diagnostic files are not restored.
Bounds/failure policy: current README and deploy ADR-017. Actual multi-owner, memory and fault evidence:
`tests/results/2026-11-22-sprint-07-hardening/`.

## Test/evidence to maintain after implementation

- Contract version and related ADR:
- Input and output examples:
- Success and error tests:
- Exact command/run timestamp:
- Observed metrics/logs (or NOT RUN):
- Known limitations:
- Next action:

See `nexstream-deploy/docs/` for end-to-end behavior.
