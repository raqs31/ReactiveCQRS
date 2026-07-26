# Operations

Running a ReactiveCQRS application: what gets created in your database, what you can tune, and what
the framework assumes about your deployment.

## Database setup

An empty database and a user who owns it. The framework creates everything else on startup with
idempotent DDL (`CREATE TABLE IF NOT EXISTS`, `CREATE SEQUENCE IF NOT EXISTS`,
`CREATE OR REPLACE FUNCTION`), so the user needs `CREATE` privileges on the database.

```sql
CREATE USER reactivecqrs WITH PASSWORD '...';
CREATE DATABASE reactivecqrs OWNER reactivecqrs;
```

The connection pool is **yours to create** — the framework uses ScalikeJDBC's default singleton
pool and never initialises it. Do this before constructing anything from the framework:

```scala
Class.forName("org.postgresql.Driver")
ConnectionPool.singleton(jdbcUrl, user, password,
  ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L))
```

Size the pool for the actors that hold a connection while working: every active
`AggregateRepositoryActor` during a write, plus every projection during an update. A pool that is
too small shows up as `connectionTimeoutMillis` errors under load, not as deadlock.

## Schema reference

Everything below is created automatically.

### Event store

| Object | Purpose |
|---|---|
| `events` | The record. `id`, `user_id`, `aggregate_id`, `event_time`, `version`, `event_type_id`, `event_type_version`, `event` (JSON text). |
| `events_seq` | Sequence backing `events.id`. |
| `aggregates` | Identity and version per aggregate: `id`, `space_id`, `creation_time`, `type_id`, `base_order`, `base_id`, `base_version`. `base_version` is the current version and is what the optimistic lock compares. The `base_*` columns implement duplication chains. |
| `noop_events` | Events cancelled by an `UndoEvent`, excluded during replay. |
| `events_to_publish` | Outbox consumed by the event bus. |

Optimistic locking lives in PL/pgSQL: `add_event`, `add_undo_event`, `add_duplication_event`.

Indices created: `events(aggregate_id)`, `events(aggregate_id, id)`, `aggregates(type_id)`,
`aggregates(base_id)`.

### Event bus

| Object | Purpose |
|---|---|
| `event_bus` | Cursor of last published version per aggregate. Unique indices on `(aggregate_id)` and `(aggregate_id, aggregate_version)`. |
| `event_bus_seq` | Sequence for `event_bus.id`. |
| `events_to_route` | Messages in flight per subscriber. Unique index `(aggregate_id, version, subscriber)`. |
| `events_to_route_seq` | Sequence for the above. |

### Projections, sagas, commands, types

| Object | Purpose |
|---|---|
| `subscriptions`, `subscriptions_seq` | Per-projection progress. Index on `aggregate_id`; unique on `(subscriber_type_id, subscription_type, aggregate_id)`. |
| `projection_<name>` | Your read models. `space_id`, `id` (PK), `version`, `document` (JSONB). Auto-migrates: adds `space_id`, drops a legacy `metadata` column. |
| `sagas` | Saga progress. **No primary key and no index** — see below. |
| `commands_responses`, `commands_responses_seq` | Cached responses for idempotent commands. |
| `types_names`, `types_names_seq` | Maps class names to the small integer ids stored on events and subscriptions. |
| `components_versions` | Schema versions of the framework's own components. |

### Id sequences

| Sequence | Used by |
|---|---|
| `aggregates_uids_seq` | `AggregateId` allocation |
| `commands_uids_seq` | `CommandId` allocation |
| `sagas_uids_seq` | `SagaId` allocation |

The sequence's `increment_by` is the **pool size** — how many ids a generator takes per round
trip. Raising it reduces database traffic and, more importantly, reduces how often an id pool
drains (see [Latency spikes](#latency-spikes) below):

```sql
ALTER SEQUENCE aggregates_uids_seq INCREMENT BY 1000;
```

Ids are consumed from the pool in memory, so gaps after a restart are normal and harmless.

## Indices worth adding

`sagas` is created with no primary key and no index, and is queried by `name` on every saga-actor
start (`loadAllSagas`) and by `(name, saga_id)` on every update and delete. Neither index is
created automatically; both are safe to add yourself:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS sagas_name_id_idx ON sagas (name, saga_id);
CREATE INDEX IF NOT EXISTS sagas_name_idx ON sagas (name);
```

The outbox is already covered — `PostgresEventStoreSchemaInitializer.scala:99` creates
`events_to_publish_aggregate_idx ON events_to_publish (aggregate_id, version)`. Older notes
describing this index as missing are out of date.

For document stores, declare indices when constructing the store rather than by hand, so they
survive a rebuild — see
[guides/04-projections.md](guides/04-projections.md#indices).

## Tunables

### Event store

```scala
new PostgresEventStoreState(mpjsons, typesNamesState,
  fetchSize = 1000,                    // JDBC fetch size when streaming events
  eventSizeLimit = 100000,             // bytes of serialized JSON per event
  aggregateVersionLimit = 10000,       // max events per aggregate
  eventSizeLimitPerClassName = Map.empty)
```

Exceeding `eventSizeLimit` raises `EventTooLargeException`; exceeding `aggregateVersionLimit`
raises `TooManyEventsException`. Both are usually telling you an aggregate boundary is wrong
rather than that the limit is too low. `eventSizeLimitPerClassName` raises the limit for specific
event classes that legitimately carry more.

### Event bus

```scala
new EventsBusActor(eventBusState, eventBusSubscriptionsManager,
  MAX_BUFFER_SIZE = 10000,   // in-flight messages before back-pressure kicks in
  eventsLogger = None)
```

Back-pressure means a slow projection throttles publication rather than accumulating an unbounded
queue. Lower it if memory is tight; raise it if you have bursty writes and fast projections.

### Command bus

```scala
AggregateCommandBusActor(aggregateContext, uidGenerator, eventStoreState, commandResponseState,
  eventBusActor, eventsReplayMode = false,
  maxInactivityMillis = 43200000L,   // 12 hours
  keepAliveLimit = 200)
```

These control the cache of per-aggregate child actors. An actor evicted from the cache must replay
its aggregate's event stream on next access, so on a system with a large working set, raising
`keepAliveLimit` trades memory for latency. Lowering it does the reverse.

### Projections

See [guides/04-projections.md](guides/04-projections.md#tuning) for `ProjectionActorOptions`
(`groupUpdatesDelayMillis`, `eventsToProcessImmediately`, `parallelUpdateProcessing`).

## Aggregate size

**There are no snapshots.** Every time an `AggregateRepositoryActor` starts — first access, or
after being evicted from the command bus's cache — it replays the aggregate's *entire* event
stream to rebuild state, on the actor thread.

Consequences:

- First-access latency grows linearly with stream length.
- An aggregate that receives events indefinitely gets progressively slower, then hits
  `aggregateVersionLimit`.
- Sizing `keepAliveLimit` so hot aggregates stay resident is the main mitigation available today.

Design aggregates with a natural lifecycle. If you find yourself wanting a snapshot, that is
usually a signal that the aggregate is doing too much.

## Latency spikes

Two places block an actor thread and will show up as occasional multi-second stalls:

- **Id pool exhaustion.** When a command bus or saga actor runs out of ids, it blocks waiting for
  the generator (up to 60 seconds if the generator fails). Mitigate by raising the sequence's
  `increment_by` so pools drain rarely.
- **Persisting events.** The database transaction runs synchronously on the aggregate's actor
  thread, so that aggregate is blocked for the round trip. This is the per-aggregate write
  ceiling. Keep the database close and the pool warm.

## Multi-node deployment

**The framework assumes a single application instance.** `PostgresDocumentStore` and
`PostgresSubscriptionsState` keep in-memory caches with no cross-instance invalidation, so one
node's writes will not invalidate another node's cache.

If you must run several instances against one database:

- Use `NoopDocumentStoreCache` and `PostgresSubscriptionsState(..., keepInMemory = false)` to avoid
  the stale-read window.
- Be aware that per-aggregate serialisation is per-actor, and two nodes each have their own actor
  for the same aggregate. The optimistic lock in the stored procedures still protects correctness
  — conflicting writes are rejected — but `ConcurrentCommand` retry rates will climb.
- Run projections on one node only, so a given projection has a single writer.

Proper clustering is not implemented.

## Observability

- Logging goes through SLF4J. Set `io.reactivecqrs` to `DEBUG` to trace the actor pipeline; it is
  extremely verbose. There are a few stray `println`s in the saga and id-generator recovery paths
  that bypass the logger.
- Several components accept an optional `eventsLogger: Option[Logger]`
  (`PostgresSubscriptionsState`, `EventsBusActor`, `ProjectionActorOptions`) for targeted event
  tracing without turning on framework-wide debug.
- Useful health queries:

```sql
-- Outbox backlog: events persisted but not yet published.
SELECT count(*) FROM events_to_publish;

-- Projection lag: aggregates whose latest version is ahead of a projection's recorded position.
SELECT count(*) FROM subscriptions s JOIN aggregates a ON a.id = s.aggregate_id
WHERE s.aggregate_version < a.base_version;

-- In-flight saga count.
SELECT name, count(*) FROM sagas GROUP BY name;

-- Longest aggregates — candidates for a boundary rethink.
SELECT aggregate_id, count(*) c FROM events GROUP BY aggregate_id ORDER BY c DESC LIMIT 20;
```

## Backup and retention

The `events` and `aggregates` tables are the system of record; everything else is derived and can
be rebuilt. A backup strategy that captures those two tables consistently is sufficient for
correctness, though restoring without the rest means a full projection rebuild
([guides/06-replay.md](guides/06-replay.md)).

Do not delete from `events`. Retention, where you need it, means `PermanentDeleteEvent`
per aggregate — and note that it currently leaves orphaned rows in `noop_events` for aggregates
that had undo events.

## Upgrading the framework

The version lives in `project/Common.scala`. Schema migrations run automatically on startup via
the `initSchema()` calls and `components_versions`. Because migrations are not wrapped in a single
transaction, a failure mid-migration can leave the schema partially updated — take a backup before
upgrading a production database, and expect DDL on large `projection_*` tables to hold locks.
