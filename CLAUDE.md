# CLAUDE.md — edutap.data_models

Repository-specific rules. They take precedence over the global defaults.

## Language

**English only.** This repository belongs to eduTAP proper, not to any single
institution: README, changelog, documentation, docstrings, code comments, commit
messages, pull request titles and bodies, and replies to review comments.

The language follows the repository, not the conversation. A discussion held in
German still produces English artefacts here.

## What this package is

Shared contracts of the eduTAP packages: controlled vocabularies, the Kafka message
contract, reusable settings building blocks — and, since 0.2.0, the Kafka **runtime**
that consuming services share: the loop over a consumer, the error classification, the
dead letter queue and the process that ends when any one consumer does.

That last one widened the purpose, and the widening is deliberate rather than
accidental. A runtime loop is not a contract in the way a vocabulary is. It earns its
place because the *behaviour* is contract: the commit order, the DLQ naming, what a
stop signal does. Two services need exactly this and a third was about to copy it, and
three copies of a contract diverge. See
[`docs/superpowers/specs/2026-08-11-kafka-runtime-extraction.md`](docs/superpowers/specs/2026-08-11-kafka-runtime-extraction.md).

Every other eduTAP package may depend on it; it depends on nothing from the estate.

## Guard rails

**Three runtime dependencies, and a written reason for a fourth.** What this package
pulls in, every consumer pulls in. `pydantic`, `pydantic-settings` and `structlog` are
the budget. A dependency that would serve only one consumer belongs in that consumer.

`structlog` was the third and had to be argued for; the ceiling moved with it, the
rule did not. The reason, in writing: the runtime loop's log records are structured —
`topic`, `partition`, `offset`, `reason` — and that structure is their entire value in
operation. Routed through stdlib `logging` it would collapse into a message string.
Every consumer already uses it.

**No Kafka driver here.** The runtime is written against protocols — `Consumer`,
`Producer`, `Handler` — so `aiokafka` stays with the services that build consumers and
producers. Anything that has to import the driver (`build_consumer()`,
`build_dead_letter_producer()`) belongs in the service, and so does the service's
settings class.

**Never import from another eduTAP package.** A library that knows about services is
not a library. If something here needs a service's type, the type is in the wrong
place.

**No table definitions of individual packages.** `edutap.db_definitions` collects
schemas through entry points, and that collection is what makes one package
answerable for one schema. Shared *contracts* belong here; owned *tables* do not.

**Vocabulary values are API.** Changing a value breaks stored data in every
deployment. Add values freely, rename or remove them only with a migration path and a
major version.

**No `uv.lock`.** This is a library; pinning here would push a resolution onto every
consumer.

## Working practice

Branch first, never commit on `main`. Push only when asked. `make lint` and
`make test-local` green before opening a pull request.

Design records live under `docs/superpowers/`. They are records of a decision at a
point in time — do not rewrite them to match a later state; write a new one.

## Sources and confidentiality

**No vendor internals — from any vendor, not just the ones currently in play.**
Neither in files nor in commit messages.

The standard is academic: a statement counts as reliable only where it can be
evidenced from public information, with a link. Everything else was obtained either
by our own testing or through insider knowledge, and the three are not
interchangeable:

* **Documented** — public source, linked. May be written as fact.
* **Verified, not citable** — obtained by a person from an access-protected area and
  checked there; the reference is recorded internally but must not be published; and
  the statement has been reduced to what is not confidential. May be written as fact,
  carrying this label. It is the rule journalism uses for source protection: the claim
  stands, we know where it comes from, the reader does not get the source.

  The four conditions hold together. A statement for which nobody can name the
  internal reference does not fall here — that is insider knowledge.
* **Measured** — established by our own tests. May be written down, but always marked
  as such, because it describes what a platform did on the day we looked, not what it
  guarantees. It can change with the next release, without notice and without an
  entry in any changelog.
* **Insider knowledge** — is not written down at all.

What a platform's behaviour *means for us* stays documentable even where the
mechanism does not: "the platform enforces a deadline, it is self-healing, it is
outside our control" carries the design consequence without disclosing anything.

Contract and regulatory material is wanted and citable: eduPersonAssurance, GÉANT and
eduGAIN terms, published wallet programme obligations.
