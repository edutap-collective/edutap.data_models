# Design records (Claude/Superpowers documents)

* **Spec** (`specs/YYYY-MM-DD-<topic>-design.md`) — the design worked out in dialogue.
* **Plan** (`plans/YYYY-MM-DD-<topic>.md`) — the implementation derived from it.

| Date | Topic | Spec |
|---|---|---|
| 2026-08-07 | Purpose, boundaries and module layout of this package | [`specs/2026-08-07-edutap-data-models-design.md`](specs/2026-08-07-edutap-data-models-design.md) |
| 2026-08-11 | Moving the shared Kafka runtime here, and why `aiokafka` stays out | [`specs/2026-08-11-kafka-runtime-extraction.md`](specs/2026-08-11-kafka-runtime-extraction.md) |

The spec was written in the `lmu_edutap_dev_setup` meta repository because this one
did not exist yet; it moved here with the initial commit.
