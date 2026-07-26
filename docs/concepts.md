# Concepts

The vocabulary you need before reading anything else.

## Aggregate

An **aggregate** is a consistency boundary: a cluster of data that changes together and is
protected by the same invariants. In ReactiveCQRS an aggregate has:

- an **aggregate root** — a plain immutable case class holding current state
  (`BankAccount(owner, balance)`);
- an **`AggregateId`** — a globally unique `Long`, allocated by the framework from a Postgres
  sequence;
- an **`AggregateVersion`** — an `Int` starting at zero, incremented by exactly one per persisted
  event.

The root is held as `Option[AGGREGATE_ROOT]`. `None` means deleted — an event handler returning
`null` is how you get there.

**One aggregate is one unit of serialisation.** All writes to a given aggregate are funnelled
through a single actor, so they happen one at a time. This is what makes invariants enforceable
without locks, and it is also the write throughput ceiling for that aggregate.

Choose aggregate boundaries accordingly: an aggregate that everything in the system touches
becomes a queue. And because there are no snapshots, an aggregate that accumulates events forever
becomes slow to load — see [operations.md](operations.md#aggregate-size).

## Event

An **event** is a fact: something that happened, named in the past tense. Events are the system of
record. Nothing else is — the aggregate root is a cache of the events, rebuilt by replaying them.

Consequences worth internalising:

- **Events are immutable and permanent.** Once persisted, an event is a published schema. See
  [guides/03-events.md](guides/03-events.md#evolving-an-event) before changing an event class.
- **Event handlers must be pure and total.** They run on every replay. No I/O, no clock, no
  randomness, no validation, no exceptions.
- **You cannot "correct" an event by editing it.** You emit a compensating event, or — for genuine
  erasure — use the history-rewrite or permanent-delete escape hatches, which sacrifice the audit
  trail.

There are five kinds, covered in [guides/03-events.md](guides/03-events.md):

| Kind | Meaning |
|---|---|
| `Event` | An ordinary fact. |
| `FirstEvent` | Creates the aggregate. Must supply a `SpaceId`. |
| `UndoEvent(eventsCount)` | Logically cancels the previous *n* events. |
| `DuplicationEvent` | Creates a new aggregate as a copy of another, at a chosen version. |
| `PermanentDeleteEvent` | Hard-deletes the aggregate and its events. Irreversible. |

## Command

A **command** is a request: something someone wants to happen, which may be refused. Commands are
named in the imperative (`Withdraw`, `OpenAccount`).

A command handler receives current aggregate state and returns either `CommandSuccess(events...)`
or `CommandFailure(reasons)`. It is the *only* place validation belongs, and it must not perform
side effects — returning an event is the effect.

The command's **flavor** decides how it is routed and how concurrency is handled:

| Flavor | Concurrency |
|---|---|
| `FirstCommand` | Creates an aggregate; the bus allocates the id. |
| `Command` | Carries `expectedVersion`. Mismatch ⇒ `AggregateConcurrentModificationError`, nothing persisted. |
| `ConcurrentCommand` | No expected version; runs against current state and is **retried automatically** on conflict. |
| `RewriteHistoryCommand` / `RewriteHistoryConcurrentCommand` | Rewrites already-persisted events. |

`IdempotentCommand` is orthogonal to all of these: when `idempotencyId` is set, the response is
cached and a replay of the same key returns the stored response instead of executing again.
See [guides/02-commands.md](guides/02-commands.md).

## Version

`AggregateVersion` is a monotonic counter, one per persisted event. It is the concurrency-control
mechanism and the coordinate system for history:

```scala
GetAggregate(id)                                // current state
GetAggregateForVersion(id, AggregateVersion(7)) // state after the 7th event
GetAggregateAtInstant(id, instant)              // state as of a point in time
GetAggregateMinVersion(id, version, maxMillis)  // wait until the aggregate reaches a version
```

The last one is the antidote to eventual consistency in request/response code: rather than
sleeping and hoping, you ask for "this aggregate, at version ≥ 8" and the repository actor holds
your query until it can answer or the timeout expires.

## Space

A **`SpaceId`** is a coarse partitioning key — tenant, workspace, customer, organisation. Every
aggregate belongs to exactly one space, fixed by its `FirstEvent` at creation and immutable
thereafter. Document stores carry `space_id` too, so read models can be scoped or purged per
space.

If you have no partitioning scheme, `SpaceId(0)` is the conventional "everything" space. Choosing
this well up front is worth some thought, because it cannot be changed later without rewriting
history.

## Projection

A **projection** is a read model derived from the event stream, maintained by a `ProjectionActor`
and stored in a **document store** (a Postgres table of JSONB documents). Projections exist so
that queries do not have to load and replay aggregates.

Two properties define how you must write them:

- **Eventually consistent.** A command returns when its events are persisted; the event bus
  delivers to projections afterwards.
- **Idempotent.** The bus may redeliver, and a rebuild replays everything from the beginning.
  Applying the same event twice must not corrupt the model.

A projection declares a `version`; bumping it signals that the read model's shape changed and it
needs rebuilding from history. See [guides/04-projections.md](guides/04-projections.md) and
[guides/06-replay.md](guides/06-replay.md).

## Saga

A **saga** (process manager) coordinates a change spanning several aggregates. There are no
distributed transactions: each step is its own local transaction, and failure is handled by
**compensating** — issuing new commands that undo earlier steps — not by rolling back.

Saga progress is persisted before each step, so a crash mid-process is resumed on restart. That
is why every command a saga issues carries a `SagaStep` as its idempotency key: on resume a step
may be re-issued, and the cached response prevents the effect happening twice. See
[guides/05-sagas.md](guides/05-sagas.md).

## The consistency model, stated plainly

This is the part that most often surprises people:

| Read | Consistency |
|---|---|
| `GetAggregate` and friends, via the command bus | **Strong.** Served by the actor that owns the aggregate. |
| A projection / document store | **Eventual.** Lags by however long publication and processing take. |
| A saga's view of other aggregates | **Eventual**, and each step sees a possibly-changed world. |

Within a single aggregate you get serialisability and optimistic locking. Across aggregates you
get neither, by design — that is what sagas and compensation are for.

## Where the data lives

| Concept | Table(s) |
|---|---|
| Events | `events`, `noop_events` |
| Aggregate identity, version, space, duplication chains | `aggregates` |
| Outbox for publication | `events_to_publish` |
| Event bus cursor and routing | `event_bus`, `events_to_route` |
| Projection progress | `subscriptions` |
| Read models | `projection_<name>` |
| Saga progress | `sagas` |
| Idempotent command responses | `commands_responses` |
| Id allocation | `aggregates_uids_seq`, `commands_uids_seq`, `sagas_uids_seq` |

All of it is created automatically. Details in [operations.md](operations.md).

Every one of these has a memory-backed counterpart (`MemoryEventStoreState`, `MemoryEventBusState`,
`MemorySubscriptionsState`, `MemoryDocumentStore`, …) except saga state, so a complete system can
run with no database for exploration and tests — see
[Running without a database](getting-started.md#running-without-a-database).
