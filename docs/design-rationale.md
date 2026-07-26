# Design rationale

Why ReactiveCQRS is built the way it is, and what its more unusual mechanisms actually do.

> **On sourcing.** Everything here is derived from reading the code — the mechanisms are described
> from the SQL and Scala that implement them, with `file:line` references. The *motivations* are
> reconstructed from what the code optimises for; they are informed inference, not statements from
> the authors. Where a design has a cost, it is stated.

## The shape of the thing

Three decisions define the architecture, and most other properties follow from them.

1. **One actor owns one aggregate instance.** All writes to an aggregate funnel through a single
   `AggregateRepositoryActor`, so they are serialised without locks in application code.
2. **PostgreSQL is not just storage, it is the concurrency-control mechanism.** The optimistic
   lock lives in PL/pgSQL, not in Scala.
3. **The event log and the outbox are written in the same transaction.** Publication can never
   disagree with persistence.

## Why the optimistic lock is a stored procedure

The obvious Scala implementation — read version, decide, `INSERT ... WHERE version = ?`, check the
row count, retry — needs a round trip per attempt and gets the retry semantics subtly wrong under
contention.

`add_event` does it in one call
(`core/.../PostgresEventStoreSchemaInitializer.scala:139-172`):

```sql
UPDATE aggregates SET base_version = base_version + 1
  WHERE id = aggregate_id AND base_id = aggregate_id
  RETURNING base_version - 1 INTO current_version;
IF NOT FOUND THEN
    IF expected_version = 0 AND space_id >= 0 THEN
        INSERT INTO aggregates (...) VALUES (..., base_order 1, base_id aggregate_id, base_version 1);
        current_version := 0;
    ELSE
        RAISE EXCEPTION 'aggregate not found, ...';
    END IF;
END IF;
IF expected_version >= 0 AND current_version != expected_version THEN
    RAISE EXCEPTION 'Concurrent aggregate modification exception, ...';
END IF;
```

What this buys:

- **The version bump and the lock are the same operation.** The `UPDATE` takes a row lock, so a
  second writer *waits* rather than racing. The version comparison then happens against a value
  nobody else can be changing.
- **Creation is the same code path.** `NOT FOUND` plus `expected_version = 0` means "this is a
  `FirstEvent`", so there is no separate create path to keep consistent.
- **`expected_version = -1` skips the check.** That single sentinel is how `ConcurrentCommand`
  works at the storage layer — take whatever version the increment produced.
- **One round trip.** Version check, event insert and outbox insert are one call, one transaction.

The cost: the write path is Postgres-specific and cannot be swapped for another store without
reimplementing these procedures. "Externalize datastore" is on the [roadmap](roadmap.md).

Version 0.12.45 added `add_events`, which batches several events for one aggregate into a single
round trip while preserving the per-event lock semantics
(`PostgresEventStoreSchemaInitializer.scala:174-181`).

## Why there are no snapshots

This is the design's most consequential omission, and the one to understand before adopting it.

Every `AggregateRepositoryActor` rebuilds state by replaying the aggregate's entire event stream on
start. Loading cost is O(number of events), forever.

Why it might have been chosen:

- **Snapshots are a second serialization format to version.** A snapshot of aggregate state has to
  survive refactoring of the aggregate root — a separate evolution problem from event versioning,
  and one that is easy to get wrong in a way that silently corrupts state.
- **The actor cache blunts the cost.** `AggregateCommandBusActor` keeps up to `keepAliveLimit`
  (default 200) child actors alive for `maxInactivityMillis` (default 12 hours), so a hot aggregate
  replays once per half-day, not once per command.
- **A guardrail is in place.** `aggregateVersionLimit` (default 10 000) makes an unbounded stream
  fail loudly rather than degrade silently
  (`core/.../PostgresEventStoreState.scala:16, 55-56`).

The cost is real and unavoidable: first-access latency grows with history, and it grows on the
actor thread. The design pushes you toward aggregates with bounded lifecycles. If your domain has
a genuinely long-lived aggregate, this framework will fight you.

## `UndoEvent`: undo without reversal logic

Most event-sourced systems handle "undo" by requiring the developer to emit an inverse event —
`MoneyDeposited(100)` compensated by `MoneyWithdrawn(100)`. That works but pollutes the log with
corrections and puts the burden on every event type.

ReactiveCQRS instead marks events as no-ops and excludes them at replay. `add_undo_event` inserts
rows into `noop_events`, and every read joins them out
(`core/.../PostgresEventStoreState.scala:207-218`):

```sql
LEFT JOIN noop_events ON events.id = noop_events.id
                     AND noop_events.from_version <= aggregates.base_version
```

The `from_version` column is the clever part. A no-op is not absolute — it applies only once the
aggregate has reached that version. That means:

- undo is itself recorded as an event and is auditable;
- **an undo can be undone**, because reading the aggregate at a version before the undo event still
  sees the original events;
- nothing is ever deleted.

The cost: every event read carries a `LEFT JOIN`, and your event handler must still accept the
undo event (returning state unchanged), which is an easy `MatchError` to trip over. The framework
even carries a `TODO` noting this event should not need handling.

## Duplication chains: copy-on-write aggregates

`DuplicationEvent` creates a new aggregate that *shares* the base aggregate's history up to a
chosen version, then diverges. No events are copied.

The mechanism is in the schema. `aggregates` has a composite primary key `(id, base_id)`, so one
logical aggregate can have **several rows** — one per link in its chain — carrying `base_order`,
`base_id` and `base_version`. Reads join across the whole chain and order by it
(`core/.../PostgresEventStoreState.scala:209-211`):

```sql
JOIN aggregates ON events.aggregate_id = aggregates.base_id
               AND events.version <= aggregates.base_version
WHERE aggregates.id = ?
ORDER BY aggregates.base_order, version
```

So replaying a duplicate walks the base's events up to the fork point, in `base_order`, and then
its own. Duplication is O(1) in storage regardless of how much history is shared.

This is genuinely unusual — I am not aware of another event-sourcing framework that models
copy-on-write aggregates in the store itself. It makes sense for domains built around templates,
"save as copy", or what-if scenarios, where duplicating a rich object is a first-class user
action.

The cost: reads traverse the chain, every event query carries the join, and a long chain of
duplicates-of-duplicates compounds it. It also complicates the read SQL considerably, which is
where the framework's more intricate queries live.

## Why command flavors instead of a retry policy

Most frameworks expose one command type and handle concurrency with configuration — a retry count,
a backoff policy. ReactiveCQRS makes it a type-level choice: `Command` carries `expectedVersion` and
rejects; `ConcurrentCommand` does not and retries.

The argument for this is that the two are not the same operation with different tuning. They differ
in what the *caller* knows:

- A user acted on a screen showing version 7. If the aggregate is now at version 9, silently
  retrying applies their decision to a world they never saw. Rejection is correct.
- A saga step or background job has no such context. Rejection just means the work does not happen.
  Retrying against fresh state is correct.

Making it a type means the choice is made once, at the command's definition, by someone thinking
about the domain — rather than in configuration, later, by someone thinking about flakiness.

The retry re-runs your *handler*, not the events
(`core/.../CommandExecutorActor.scala:78-90`), which is what makes it safe: a `Withdraw` retried
against a drained balance correctly fails.

The cost: five command base classes to learn, and the `PartialFunction` dispatch means a command
that extends the wrong one fails at runtime rather than compile time.

## Why `SpaceId` exists

Multi-tenancy retrofitted onto an event store is painful — every query needs a tenant predicate,
and the tenant of an aggregate has to be derivable without loading it.

`SpaceId` is captured on the `FirstEvent`, stored on the `aggregates` row, and carried through to
document stores. It is deliberately opaque: the framework never interprets it, it just partitions
by it.

The cost: it is immutable after creation, and there is no framework-level enforcement that queries
are scoped by it — that is left to the application. `SpaceId(0)` as the "no partitioning" default
is a decision that is expensive to revisit once you have a lot of aggregates in space zero.

## Why blocking appears where it does

Several places block an actor thread, which looks like an oversight in a reactive framework. Two
are deliberate trade-offs and one is acknowledged debt.

- **Persisting events runs synchronously on the actor thread**
  (`AggregateRepositoryActor` persist path). This is *why* the single-writer guarantee holds
  without additional coordination: the actor cannot process the next command until this one is
  durable. Moving it off-thread would require reintroducing ordering machinery. The cost is that
  per-aggregate write throughput is bounded by database round-trip latency.
- **Id pool exhaustion blocks with `Await.result(..., 60s)`**
  (`AggregateCommandBusActor.scala:315-348`, `SagaActor.scala:136-152`). This one is debt — the
  code carries a `TODO` to remove the ask pattern. It is mitigated by pool size: raising a
  sequence's `increment_by` makes exhaustion rare. It is not eliminated.
- **Subscription init blocks once at startup** (`EventsBusActor.scala:187-191`). One-shot, low
  risk.

## Why projections are actors with their own query handling

A projection could have been a plain function plus a query API over the document store. Making it
an actor means updates and queries are serialised against each other, so a query never observes a
half-applied batch. It also gives the framework somewhere to put the out-of-order and delay
buffers that at-least-once delivery requires.

The cost is that `receiveQuery` runs on the actor thread, so an expensive query blocks projection
updates — which is why the guide pushes filtering into the document store rather than into Scala.

## Things that look like bugs but are choices

| Looks wrong | Actually |
|---|---|
| `eventHandlers` returns `null` to delete | Deliberate: the framework stores `Option`, and `null` is the only value a `PartialFunction[Any, ROOT]` can return to mean "gone" without changing the signature |
| `initialAggregateRoot` is never persisted | It exists so `FirstCommand` handlers have a non-null value to match against |
| `UndoEvent` must be handled but changes nothing | The framework applies the undo via `noop_events`; the partial function still has to be total |
| `commandHandlers` / `eventHandlers` are `PartialFunction`s | Cost of the design: dispatch is by runtime type, so exhaustiveness is not checked |
| `AggregateVersion` pools instances below 1000 | Deliberate allocation avoidance on a very hot type (`api/.../AggregateVersion.scala`) |

## Things that look like choices but are debt

Recorded honestly, with detail in section 7 of `CLAUDE.md`:

- The event bus's batch cursor update ignores its affected-row count, so a concurrent update can
  silently leave the cursor stale (`EventBusState.scala:138-140`, with an in-code
  `TODO check if all updates occured`).
- `sagas` has no primary key and no index, so `loadAllSagas` scans on every saga-actor start
  (`PostgresSagaSchemaInitializer.scala:12`). SQL to add them is in
  [operations.md](operations.md#indices-worth-adding).
- Saga persistence failures `throw` inside a `Future` callback, off the actor thread, where
  supervision cannot see them (`SagaActor.scala`, four occurrences).

Two items that older notes list as defects have since been fixed in the code, and are recorded
here so they are not "re-fixed":

- `PermanentDeleteEvent` now deletes `noop_events` **before** `events`, with a comment explaining
  the subquery dependency (`PostgresEventStoreState.scala:319-322`).
- `events_to_publish` **is** indexed — `events_to_publish_aggregate_idx (aggregate_id, version)`
  at `PostgresEventStoreSchemaInitializer.scala:99`.

## Next

- [comparison.md](comparison.md) — how these choices compare to other frameworks
- [background.md](background.md) — the patterns being implemented
