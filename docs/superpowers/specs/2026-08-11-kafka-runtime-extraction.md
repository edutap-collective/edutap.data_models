# Extracting the Kafka runtime into `edutap.data_models`

**Date:** 2026-08-11
**Status:** accepted
**Affects:** `edutap.data_models` 0.2.0, `lmu_edutap_worker`, `lmu_edutap_vzd`

The loop that reads a Kafka topic, classifies a failure, parks what cannot be
processed and ends the process when any one consumer stops now lives in this package,
under `edutap.data_models.runtime`. It was written in `lmu_edutap_worker` and stays
importable from there only until that service is switched over.

## Why the threshold was the third user

The first copy is code. The second is a judgement call — two copies can be kept in
step by whoever wrote both, for a while. The third is a decision one is making without
noticing, because by then nobody knows which of the three is the one that is right.

The spooler in `lmu_edutap_vzd` was about to be the third. The `lmu_edutap_worker`
scaffold had the loop, the Google callback handler has its own variant of the same
idea, and a spooler consuming `person` needs exactly the worker's version. Copying it
once more would have been the moment the commit order stopped being one decision and
became three implementations of it.

Deferring the extraction until the spooler existed was considered and rejected: the
divergence starts at the copy, not at the edit. What moves cleanly today is a diff of
import lines; in three months it is a reconciliation.

## Why a runtime loop is allowed in a package of contracts

This package carried contracts: vocabularies, the message header block, settings
building blocks. A loop is a different kind of thing, and pretending otherwise would
be how a shared package turns into a shared everything.

The argument for admitting it is narrow, and it is worth stating so a later reader can
check whether it still holds. The *behaviour* of this loop is contract in the same
sense the header block is — several services have to agree on it, and a service that
disagrees is a bug that shows up in production and nowhere else:

* **The commit order.** The dead letter entry is written and confirmed *before* the
  offset is committed. A service that commits first loses messages on a crash between
  the two, and loses them silently.
* **The dead letter naming.** One DLQ per input topic, `<topic>.dlq`. A service that
  names them per producer splits a topic's failures across topics nobody is watching.
* **What a stop signal does.** SIGTERM leaves the group cleanly and exits zero, so a
  rolling update does not read as a crash. A service that exits non-zero on SIGTERM
  makes every deployment look like an incident.
* **The error classification.** `Unprocessable` is parked on the first attempt,
  everything else is retried. This one has a second, sharper reason to be shared: two
  copies of the class are two *types*, and an `except Unprocessable` in one package
  does not catch the other package's. No unit test anywhere would notice.

What did **not** move is everything that is a service's own answer rather than a
shared one. That line is drawn below, and it is drawn by a dependency rule rather than
by taste.

## What moved and what did not

| Moved to `edutap.data_models.runtime` | Stayed in the service |
|---|---|
| `Unprocessable` | `build_consumer()` |
| `Consumer`, `Producer`, `Handler`, `DeadLetter` protocols | `build_dead_letter_producer()` |
| `consume()` — the loop, retry and parking | `consumer_options()` |
| `DeadLetterQueue` — the mechanics, without its factory | `make_runner()` |
| `dead_letter_options()`, taking a broker address | the settings class |
| `serve()`, `run_until_one_stops()`, `install_signal_handlers()` | which schema and which actions a topic allows |

Two of those rows deserve a note.

**`consumer_options()` stayed, `dead_letter_options()` moved.** They look symmetric and
are not. `dead_letter_options()` needs one value, the broker address, and it is passed
in. `consumer_options()` needs a group id, and the group id is derived from the
service's environment prefix by a property on the service's settings class — the group
is a statement about *which* service is reading, which is exactly the knowledge a
library must not have.

**The three tests covering `consumer_options()` stayed behind with it.** Everything
else in `tests/test_consumer.py`, `tests/test_runner.py` and `tests/test_error_path.py`
moved with the assertions unchanged, which is what makes them evidence that the
extraction changed no behaviour. `test_error_path.py` carries the load here: it is the
only place that runs `consume()` *with* a dead letter queue, so it is what holds the
commit order — dead letter entry first, offset second — to its promise.

## Why `aiokafka` stays out

The guard rail in `CLAUDE.md` — what this package pulls in, every consumer pulls in —
is what shaped the split, not a preference for protocols in the abstract. A Kafka
driver in this package would land in every eduTAP service, including the ones that
never touch a broker, and it would arrive with a version constraint they would then
have to live with.

The worker's code already had the seam in the right place: `DeadLetterQueue` was
written against a `Producer` protocol and `consume()` against a consumer protocol, so
only the factories ever imported `aiokafka`. Extraction was therefore a matter of
cutting along a line that was already drawn.

`structlog` became the third runtime dependency, which the guard rail permits only
with a written reason. It is this: the loop's log records are structured — `topic`,
`partition`, `offset`, `reason` — and that structure is their entire value in
operation. Routed through stdlib `logging` it would collapse into a message string.
Every consumer already depends on it, so nothing new arrives in any deployment.

## The one behaviour that did change: the dead letter entry

The move was the right moment to fix something the worker had not yet deployed, so
there is no stock of entries this could break.

The entry used to be a JSON envelope: origin coordinates, reason, and the payload as
`value_b64`. It is now the message itself — same `value`, same `key`, same headers —
with the diagnosis added in headers under an `edutap-dlq-` prefix of their own.

The base64 was the answer to a self-inflicted problem. It was needed only because the
payload had to fit inside a JSON object, and a body that is not valid UTF-8 does not.
Pass the bytes through and the question does not arise. What that buys:

* **A replay is a copy.** Read the entry, produce it back onto its original topic,
  done. The envelope needed a tool that unpacks, decodes and rebuilds the headers —
  and that tool is a second place where the envelope has to be understood correctly.
* **The entry is readable.** `kcat` and the Kafka UI show the content directly.
  With the envelope one sees base64 — at precisely the moment somebody under pressure
  is looking to find out what broke.
* **A tombstone stays a tombstone.** `record.value is None` is passed through as
  `None` instead of becoming `""`. The envelope could not express the difference.

The diagnostic headers sit under their own prefix so they cannot collide with the
original block, which is passed through untouched: `edutap-producer` still names who
*sent* the message rather than who parked it, and `edutap-event-id` still identifies
the message rather than the entry. Who parked it is `edutap-dlq-parked-by`, and it is
constructor state — `DeadLetterQueue(producer, parked_by=...)` — because that is the
service's answer to give, not the library's.

## Consequences

* Services reference `v0.2.0` by tag, never by branch. A branch reference would let
  somebody else's merge change a service's build with no line changing in it.
* `lmu_edutap_worker` deletes its five modules and imports from here. That switch has
  to be merged **before** the spooler starts depending on this package: otherwise two
  `Unprocessable` classes exist as distinct types, and an `except Unprocessable` fails
  to catch the other one's — invisible to every unit test.
* The Google callback handler has its own variant of this loop. It is not in scope
  here; if it is aligned later, it aligns to this.
