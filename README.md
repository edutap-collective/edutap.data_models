# edutap.data_models

Shared contracts of the eduTAP packages: controlled vocabularies, the Kafka message
contract, and settings building blocks whose field names are meant to be identical
in every container.

## Why this exists

Three duplicates, all of them found in the field.

**Vocabularies in three diverging copies.** `PassLifecycleState` existed with eight
values in `lmu_edutap_common` and with six in both `lmu_edutap_full_view` and
`edutap.data_provider`. `WalletType` existed in four spellings.

**The message contract.** Header block, key rule and dead letter envelope have to be
identical in six packages. Six copies diverge, and the divergence is invisible until
a consumer quietly stops recognising a message.

**Settings that should mean the same thing everywhere.** `environment` is the clearest
case: the same field name in every container, populated per service by the Swarm
compose file, and read by the error tracker and the trace exporter alike.

## What belongs here

| Module | Contents |
|---|---|
| `vocabulary` | `WalletType`, `IssuanceState`, `HolderState`, `InstanceState`, `FieldKind`, `Provider` |
| `messaging` | header names and construction, logical topic names, DLQ naming |
| `settings` | `ServiceSettings`, `KafkaSettings` — mixins, not a finished class |

The pass lifecycle is spelled on **three** axes. `IssuanceState` is what the issuer
did or wants and exists with no exemplar at all; `InstanceState` is what one exemplar
at the holder is doing; `HolderState` is the summary of the second and is derived,
never set. `PassLifecycleState` conflated the first two and is superseded — it is
still reachable under `edutap.data_models.vocabulary` and warns when used.

Error tracking and trace export live in `edutap.observability_settings`, not here.
The options that decide what may leave a process were chosen against measurements
and are worth nothing apart from the `sentry_sdk.init()` call that applies them.

## What does not

Table definitions of individual packages. `edutap.db_definitions` collects those
through entry points, and that collection is what makes one package answerable for
one schema. Move them here and nobody owns a schema any more.

Anything LMU-specific. `lmu_edutap_common` stays what it is; the `edutap.*` packages
must not depend on it.

## Dependency direction

This package depends on nothing from the eduTAP estate — only on `pydantic` and
`pydantic-settings`. Everything else may depend on it. A library that knows about
services is not a library.

## Usage

```python
from edutap.data_models import WalletType
from edutap.data_models.messaging import TOPIC_PASS_STATE, build_headers
from edutap.data_models.settings import KafkaSettings


class MySettings(KafkaSettings):
    model_config = {"env_prefix": "EDUTAP_MY_SERVICE_"}


settings = MySettings()                      # aborts without EDUTAP_MY_SERVICE_TOPIC_PREFIX
topic = settings.topic(TOPIC_PASS_STATE)     # edutap.production.pass.state
```

## Development

```shell
make venv
make lint
make test-local
```

`tox` runs the suite across every supported Python version.

## Design records

The design is written down under [`docs/superpowers/specs/`](docs/superpowers/specs/).
