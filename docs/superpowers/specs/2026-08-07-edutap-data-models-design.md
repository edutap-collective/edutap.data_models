# `edutap.data_models` — design

**Date:** 2026-08-07
**Status:** draft

A package for the contracts that **more than one** eduTAP package has to agree on:
vocabularies, the message contract, and reusable settings building blocks.

## Why

Three kinds of duplicate, all of them observed rather than imagined.

**Vocabularies in three diverging copies.** `PassLifecycleState` exists in
`lmu_edutap_common` (eight values, three state machines conflated), in
`lmu_edutap_full_view` and in `edutap.data_provider` (six each). `WalletType`
likewise, in four spellings. The docstring of `edutap.data_provider.vocabulary`
already records that its spellings supersede the older ones and that aligning them
is follow-up work.

**The message contract.** Header block, key rule and dead letter envelope have to be
identical across six packages. Six copies diverge, and the divergence stays invisible
until a consumer quietly stops recognising a message.

**Settings meant to be identical everywhere.** `sentry_dsn` is the clearest case: the
same field name in every container, populated differently by the Swarm compose file.
Today the same idea appears twice — as `LMU_EDUTAP_SENTRY_DSN_FILE`, a Docker secret
on one service, and as `GOOGLE_CALLBACK_SENTRY_DSN`, a plain variable in the
production overlay of another.

## What belongs here

| Module | Contents |
|---|---|
| `vocabulary` | `PassLifecycleState`, `WalletType`, `FieldKind`, `Provider` — the controlled values |
| `messaging` | header names and construction, key rule, dead letter envelope, logical topic names |
| `settings` | reusable pydantic-settings building blocks: `SentrySettings`, `KafkaSettings` (bootstrap, topic prefix, consumer group) |
| `contracts` | the payload contracts, starting with `pass-state/v1` |

### The settings building blocks

Mixins rather than a finished settings class — every package inherits what it needs
and keeps its own `env_prefix`:

```python
class KafkaSettings(BaseSettings):
    bootstrap_servers: str
    topic_prefix: str                    # no default -- start must abort
    consumer_group: str | None = None

    def topic(self, name: str) -> str:
        return f"{self.topic_prefix}.{name}"
```

`topic_prefix` deliberately has no default: a missing value must abort the start
rather than let a service write quietly into another environment's topics.

## What does not belong here

**Table definitions of individual packages.** `edutap.db_definitions` relies on each
package bringing its own schema and announcing it through an entry point — that is
where ownership comes from, and with it the collision check. Pull the tables into a
shared package and no schema has an owner any more.

```{note}
The tables of `edutap.data_provider` are a possible exception: `person_view` and
`pass_state` are a **contract** for external consumers, not the implementation detail
of one service. Whether they therefore belong here is open, and is to be decided
together with the question of schema versus prefix.
```

**Anything LMU-specific.** `lmu_edutap_common` stays what it is; the `edutap.*`
packages must not depend on it.

## Dependency direction

`edutap.data_models` depends on **nothing** from the eduTAP estate — only on
`pydantic` and `pydantic-settings`. Everything else may depend on it. A library that
knows about services is not a library.

That this couples packages inside `edutap-collective` is the price paid knowingly.
Someone using only the Google callback handler pulls it in too — acceptable at this
size, measured against six diverging copies of the header contract.

## Open points

* **Sentry or Bugsink** — or both during a transition. Two targets need two fields,
  not one.
* **Migration order** — which packages move first. Suggestion: `edutap.data_provider`,
  because its vocabulary is already the leading one.
* **Relationship to `edutap.db_definitions`** — both are cross-package. Keep them
  apart: `db_definitions` is a **tool** with no runtime role, `data_models` a
  **runtime library**.

## House style

As in `edutap.data_provider`: Makefile, tox across the supported Python versions,
ruff, ty, Renovate as the hosted app, **no** `uv.lock` (this is a library), design
records under `docs/superpowers/`.
