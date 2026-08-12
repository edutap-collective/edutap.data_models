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
| `runtime` | the shared Kafka runtime: `consume()`, `Unprocessable`, `DeadLetterQueue`, `serve()` |

The `runtime` package is the odd one out and says so. It is behaviour rather than a
contract in the way a vocabulary is, and it is here because the behaviour is what
several services have to agree on: the commit order, the dead letter naming, what a
stop signal does. It is written against protocols, so the Kafka driver is **not** a
dependency of this package — `build_consumer()` and `build_dead_letter_producer()`
stay in the service, with its settings class. See the design record of 2026-08-11.

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

This package depends on nothing from the eduTAP estate — only on `pydantic`,
`pydantic-settings` and `structlog`. Everything else may depend on it. A library that
knows about services is not a library.

`structlog` is the third and had to be argued for: the runtime loop's log records are
structured — `topic`, `partition`, `offset`, `reason` — and that structure is their
entire value in operation. Every consumer already uses it.

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

Reaching a broker that requires mTLS is the same three settings fields everywhere —
`ca_file`, `cert_file`, `key_file`, plus `password` for an encrypted key — and one
function that turns them into driver options:

```python
from edutap.data_models.runtime import transport_options

AIOKafkaConsumer(topic, bootstrap_servers=..., **transport_options(settings))
```

It returns `{}` when nothing is configured, which is the development case against a
broker without TLS. Configuring only *some* of the three raises instead of falling
back: against an SSL-only broker a silent fallback does not produce an unencrypted
connection, it produces one that fails during the handshake — and that error names
the broker rather than the secret nobody mounted.

The context is built from the standard library, so the Kafka driver stays out of this
package's dependencies.

## Development

```shell
make venv
make lint
make test-local
```

`tox` runs the suite across every supported Python version.

## Design records

The design is written down under [`docs/superpowers/specs/`](docs/superpowers/specs/).
