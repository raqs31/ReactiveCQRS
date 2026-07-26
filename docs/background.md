# Background: CQRS and Event Sourcing

Context for readers who are new to these patterns, and a map from the standard vocabulary onto
ReactiveCQRS's types. The [guides](guides/01-aggregates.md) tell you how to use the library; this
page is about the ideas it implements and where they come from.

> **On sourcing.** Claims about *ReactiveCQRS* in this document were verified against the code in
> this repository and cite `file:line`. Claims about the *patterns* are drawn from the literature
> linked at the bottom. Those links were located by search but could not be retrieved and read
> directly in the environment where this page was written, so treat the external material as
> pointers for your own reading rather than as quotations. Nothing external is quoted verbatim.

## CQS, then CQRS

The root idea is Bertrand Meyer's **Command–Query Separation**: a method should either change
state or return a value, never both. Asking a question should not change the answer.

**CQRS** — Command Query Responsibility Segregation, a term coined by Greg Young — lifts that
distinction from methods to whole models. Instead of one model serving both reads and writes, you
have two:

- a **write model** shaped around invariants and transactions,
- one or more **read models** shaped around the queries the application actually asks.

The payoff is that the two have genuinely different requirements. A write model wants a small
consistency boundary and strict validation. A read model wants denormalised, query-shaped data and
does not care about invariants at all. Forcing both through one set of tables is what produces the
familiar ORM-shaped compromise.

The cost is that the two models must be kept in sync, and in practice that synchronisation is
asynchronous — which is where eventual consistency enters, and why it is a *consequence* of CQRS
rather than a bonus feature.

In ReactiveCQRS the split is explicit and structural:

| Side | Type |
|---|---|
| Write model | `AggregateContext` + `AggregateCommandBusActor` |
| Read model | `ProjectionActor` + `DocumentStore` |

## Event Sourcing

CQRS says nothing about how the write model persists itself. **Event Sourcing** is the choice to
store the sequence of state *changes* rather than current state, and to derive current state by
replaying them.

The consequences that matter:

- **The event log is the system of record.** Everything else — aggregate state, read models,
  reports — is a derived cache that can be deleted and rebuilt.
- **History is queryable.** State at any past version or instant is reconstructible, because it is
  just a replay that stops early.
- **Events are permanent and therefore a published schema.** This is the obligation people
  underestimate. See [Schema evolution](#schema-evolution) below.
- **Event handlers must be pure and total.** They run on every replay. An event handler that calls
  the clock, or validates, or throws, means your history no longer reconstructs your present.

CQRS and Event Sourcing are independent choices that are frequently used together; ReactiveCQRS
commits to both.

## The vocabulary, mapped

| Standard term | In ReactiveCQRS | Notes |
|---|---|---|
| Aggregate / aggregate root | `AggregateContext[ROOT]`, root is a plain case class | Consistency boundary; one actor per instance serialises its writes |
| Command | `FirstCommand`, `Command`, `ConcurrentCommand` | The flavor encodes the concurrency strategy |
| Domain event | `Event`, `FirstEvent`, `UndoEvent`, `DuplicationEvent`, `PermanentDeleteEvent` | The last three are non-standard; see [design-rationale.md](design-rationale.md) |
| Event store | `PostgresEventStoreState` + `events` / `aggregates` tables | Optimistic locking implemented in PL/pgSQL |
| Optimistic concurrency / expected version | `AggregateVersion`, `expectedVersion` | Enforced inside `add_event` |
| Projection / read model | `ProjectionActor`, `DocumentStore` | Postgres JSONB documents |
| Process manager / saga | `SagaActor` | Compensation-based, not 2PC |
| Transactional outbox | `events_to_publish` table | Written in the same transaction as the event |
| Snapshot | **not implemented** | See [comparison.md](comparison.md) |

## Optimistic concurrency

The classic event-sourcing write is: read the stream, decide, append with an expected version, and
let the store reject the append if another writer got there first.

ReactiveCQRS does this inside a stored procedure. The interesting detail is that the lock is
acquired *before* the version is compared
(`core/.../PostgresEventStoreSchemaInitializer.scala:148`):

```sql
UPDATE aggregates SET base_version = base_version + 1
  WHERE id = aggregate_id AND base_id = aggregate_id
  RETURNING base_version - 1 INTO current_version;
...
IF expected_version >= 0 AND current_version != expected_version THEN
    RAISE EXCEPTION 'Concurrent aggregate modification exception, ...';
END IF;
```

So the row is locked by the `UPDATE` — serialising concurrent writers at the database level — and
the version assertion is then evaluated against the value that was there. Concurrent writers queue
on the row lock rather than racing and both failing. A `ConcurrentCommand` passes
`expected_version = -1`, which skips the assertion entirely and simply takes whatever version the
increment produced.

This is a stronger guarantee than an append-with-expected-version against an append-only log, and
it is why the write path is Postgres-specific.

## Eventual consistency and the outbox

Publishing events to read models raises the **dual-write problem**: if you commit to the database
and then publish to a bus, a crash between the two loses the event; publish-then-commit can emit
events for a transaction that rolls back.

The standard answer is the **transactional outbox** — write the message into the same database, in
the same transaction, and have a separate process relay it. Chris Richardson's pattern catalogue is
the usual reference.

ReactiveCQRS does exactly this. `add_event` writes both rows in one transaction
(`PostgresEventStoreSchemaInitializer.scala:165-166`):

```sql
INSERT INTO events (...) VALUES (...);
INSERT INTO events_to_publish (event_id, aggregate_id, version, user_id, event_time) VALUES (...);
```

`EventsBusActor` then relays from the outbox, tracks acknowledgements per subscriber, and advances
a cursor in `event_bus`. The delivery guarantee that falls out of this is **at-least-once, not
exactly-once and not globally ordered** — which is why the guides insist projections be
idempotent.

## Sagas

Once a change spans several aggregates, there is no transaction to wrap it in. The **saga** —
introduced by Garcia-Molina and Salem at SIGMOD 1987 for long-lived database transactions, and
later adopted by the microservices community for cross-service consistency — replaces rollback with
**compensation**: each step is its own committed transaction, and failure is handled by running
new transactions that semantically undo the earlier ones.

The 1987 paper's guarantee is that either all steps complete, or compensating steps run for those
that did. Note what is *not* guaranteed: isolation. Intermediate states are visible. During a money
transfer there is a real moment when the funds are in neither account.

ReactiveCQRS implements this in `SagaActor` with `CONTINUES` / `REVERTING` phases, persisting
progress to the `sagas` table before each step so a crashed process resumes. See
[guides/05-sagas.md](guides/05-sagas.md).

## Schema evolution

The literature identifies several tactics for evolving event schemas — versioned events, weak
schema, upcasting, in-place transformation, and copy-and-transform. Greg Young's *Versioning in an
Event Sourced System* is the standard treatment, and empirical work on industrial event-sourced
systems (Overeem et al.) surveys how teams actually cope.

ReactiveCQRS provides **versioned events**: `eventsVersions` in `AggregateContext` maps a stored
`(event_type_id, event_type_version)` pair to the class that can deserialize it, so an old event
class is kept and new events are written at a newer version.

```scala
override val eventsVersions = EV[AccountOpened](
  0 -> classOf[AccountOpenedV0],
  1 -> classOf[AccountOpened]) :: Nil
```

It also provides **in-place transformation**, via `RewriteHistoryCommand` — which rewrites the
stored events. That is the most invasive of the tactics and destroys the original record; the
guides recommend versioning first.

There is no upcasting middleware: you cannot register a function that transforms old JSON into the
new shape on read. You keep the old class and handle both in your event handler.

See [guides/03-events.md](guides/03-events.md#evolving-an-event).

## When these patterns are worth it

Worth stating plainly, because CQRS/ES is frequently applied where it costs more than it returns.

**Good fit:** auditability is a requirement rather than a nice-to-have; the business genuinely
thinks in events; temporal queries ("what did this look like in March?") have real value; read and
write workloads have divergent shapes; you need to add new read models over historical data.

**Poor fit:** CRUD with incidental history; strong consistency needed across many entities at once;
a small team without appetite for the operational surface (event schema governance, projection
rebuilds, eventual-consistency bugs in the UI); ad-hoc querying as the primary access pattern.

ReactiveCQRS's own constraints narrow this further — no snapshots and a single-node assumption
mean it suits aggregates with bounded lifecycles in a single-instance deployment. See
[comparison.md](comparison.md#when-not-to-use-reactivecqrs).

## Reading list

Foundational:

- [Greg Young — *CQRS Documents*](https://github.com/keyvanakbary/cqrs-documents) (community e-book edition of the original PDF)
- [Greg Young — *A Decade of DDD, CQRS, Event Sourcing*](https://virtualddd.com/videos/greg-young-a-decade-of-ddd-cqrs-event-sourcing/)
- [Greg Young — Event Sourcing, GOTO 2014](https://www.youtube.com/watch?v=8JKjvY4etTY)
- [Microsoft patterns & practices — *Exploring CQRS and Event Sourcing*](https://www.microsoft.com/en-us/download/details.aspx?id=34774) — book-length, includes a candid "what we got wrong" retrospective

Sagas:

- [Garcia-Molina & Salem, *Sagas*, ACM SIGMOD 1987](https://dl.acm.org/doi/10.1145/38714.38742) — the original paper
- [Caitie McCaffrey — *Applying the Saga Pattern*, GOTO 2015](https://gotocon.com/dl/goto-chicago-2015/slides/CaitieMcCaffrey_ApplyingTheSagaPattern.pdf)

Outbox and delivery:

- [Chris Richardson — Transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [Confluent — The Transactional Outbox Pattern](https://developer.confluent.io/courses/microservices/the-transactional-outbox-pattern/)

Schema evolution:

- [Greg Young — *Versioning in an Event Sourced System*](https://github.com/luque/Notes--Versioning-Event-Sourced-System) (community notes; the book is free online)
- [Overeem et al. — *An empirical characterization of event sourced systems and their schema evolution*](https://www.sciencedirect.com/science/article/pii/S0164121221000674)
- [Overeem et al. — *The Dark Side of Event Sourcing: Managing Data Conversion* (SANER 2017)](https://www.movereem.nl/files/2017SANER-eventsourcing.pdf)
- [Oskar Dudycz — Simple patterns for events schema versioning](https://event-driven.io/en/simple_events_versioning_patterns/)

## Next

- [design-rationale.md](design-rationale.md) — why ReactiveCQRS makes the choices it does
- [comparison.md](comparison.md) — how it compares to other frameworks
