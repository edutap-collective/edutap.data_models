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
contract, reusable settings building blocks. Every other eduTAP package may depend on
it; it depends on nothing from the estate.

## Guard rails

**Two runtime dependencies, and a written reason for a third.** What this package
pulls in, every consumer pulls in. `pydantic` and `pydantic-settings` are the budget.
A dependency that would serve only one consumer belongs in that consumer.

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

## Confidentiality

No vendor internals from Apple or NXP — not in files, not in commit messages. What a
platform's behaviour *means for us* is documentable ("the platform enforces a
deadline, it is self-healing, it is outside our control"); the mechanics, concrete
values and rule sets behind it are not.

Contract and regulatory material is fine and wanted: eduPersonAssurance, GÉANT and
eduGAIN terms, wallet programme obligations at the level they are published.
