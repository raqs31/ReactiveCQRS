# Configuration

There is no config file — everything is configured through **constructor parameters** at
wiring time. This page lists every knob with its default, plus the database objects the
framework creates.

## Prerequisites

- **ScalikeJDBC default singleton pool must be initialized first** (before constructing
  any `Postgres*` class):

  ```scala
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(url, user, password,
    ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L))
  ```

- Each `Postgres*State` has an idempotent `initSchema()` (call once at startup; safe to
  repeat). `PostgresDocumentStore` and `PostgresUidGenerator` run their schema setup in
  their constructors.

## Knobs and defaults

### AggregateCommandBusActor (factory `apply`)

| Parameter | Default | Meaning |
|---|---|---|
| `eventsReplayMode` | — (required) | `true` only in replay tooling |
| `maxInactivityMillis` | `43200000` (12 h) | Idle time before a per-aggregate child actor is eligible for reaping |
| `keepAliveLimit` | `200` | Max cached per-aggregate child actor pairs; least-recently-active reaped past this (sweep at most every 5 min) |

Fixed internals: `EnsureEventsPublished` outbox sweep 30 s after start, then every 60 s;
UID pool refill blocks the bus up to 60 s when a pool drains.

### PostgresEventStoreState

| Parameter | Default | Meaning |
|---|---|---|
| `fetchSize` | `1000` | JDBC fetch size for event streaming |
| `eventSizeLimit` | `100000` | Max serialized event size (bytes) → `EventTooLargeException` |
| `aggregateVersionLimit` | `10000` | Max events per aggregate → `TooManyEventsException` (no snapshotting — keep aggregates short-lived) |
| `eventSizeLimitPerClassName` | `Map.empty` | Per-event-class size overrides |

### EventsBusActor

| Parameter | Default | Meaning |
|---|---|---|
| `MAX_BUFFER_SIZE` | `10000` | In-flight message budget per consumer (back-pressure credits) |
| `eventsLogger` | `None` | Optional SLF4J logger for event traffic |

Fixed internals: cursor flush every 5 s; subscription init waits up to 180 s for
`EventBusSubscriptionsManager` to release subscriptions.

### EventBusSubscriptionsManager

| Parameter | Default | Meaning |
|---|---|---|
| `minimumExpectedSubscriptions` | — (required) | Number of subscribe calls to wait for before the bus starts delivering. **Must match the number of projections/sagas that subscribe**, or startup hangs. |

### ProjectionActor — `ProjectionActorOptions`

`ProjectionActorOptions.DEFAULT = ProjectionActorOptions(0, 1, Set.empty, 0, None)`; builder-style `with*` methods:

| Option | Default | Meaning |
|---|---|---|
| `groupUpdatesDelayMillis` | `0` | Debounce window for grouping updates per aggregate |
| `minimumDelayVersion` | `1` | Version below which updates are never delayed |
| `eventsToProcessImmediately` | `Set.empty` | Event classes that bypass the delay buffer |
| `parallelUpdateProcessing` | `0` | 0/1 = in-actor; >1 = fixed thread pool, sequential per aggregate |
| `eventsLogger` | `None` | Optional traffic logger |

`ListenerOptions`: `DEFAULT` (listener runs in a DB transaction with the cursor update) vs
`DISABLED_TRANSACTION` (used by the `.noDBSession` listener constructors).

### SubscribableProjectionActor

| Parameter | Default | Meaning |
|---|---|---|
| `updatesCacheTTL` | `Duration.ZERO` | How long sent updates are kept for late subscribers (missed-updates replay) |
| `subscriptionTTL` | `600000` (10 min) | Idle subscription expiry (cleanup ticks every minute; renew with `RenewSubscription`) |

### PostgresSubscriptionsState

| Parameter | Default | Meaning |
|---|---|---|
| `keepInMemory` | — (required) | `true`: memory is source of truth, persisted on `dump()` (use during replay); `false`: write-through with optimistic locking |
| `eventsLogger` | `None` | Optional logger |

### PostgresDocumentStore / PostgresDocumentStoreAutoId

| Parameter | Default | Meaning |
|---|---|---|
| `tableName` | — (required) | Becomes table `projection_<tableName>` (validated `[a-zA-Z0-9_]+`) |
| `cache` | — (required) | `DocumentStoreCache` impl; `new NoopDocumentStoreCache` to disable |
| `indicies` | `Seq.empty` | JSON-path indices (`UniqueTextIndex`, `MultipleLongIndex`, `MultipleTextArrayIndex`/GIN, …), keyed by `uniqueId` — changing the set drops/recreates indices at startup |

### Replayer

| Parameter | Default | Meaning |
|---|---|---|
| `ReplayerConfig.maxReplayerInactivitySeconds` | `30` | Per-aggregate replay actor idle self-stop |
| `ReplayerConfig.replayerTimoutSeconds` | `600` | Overall replay credit-wait timeout |
| `ReplayAllEvents.delayBetweenAggregateTypes` | — | Throttle between aggregate types (ms) |

### UID pools

Sequences created with `INCREMENT BY 100` → pool size 100 ids per DB round-trip:
`aggregates_uids_seq` (starts at 1001; first 1000 reserved), `commands_uids_seq`,
`sagas_uids_seq`. To enlarge pools, `ALTER SEQUENCE ... INCREMENT BY <n>` — the generator
reads `increment_by` at startup. Undersized pools stall the command bus/saga actor (60 s
blocking refill).

### CommandExecutorActor / SagaActor timeouts (fixed)

- Command execution self-destructs after `60 s` (`responseTimeout`).
- Saga ask timeout `60 s`; saga id-pool refill blocks up to 60 s.

## Database objects created

| Table / object | Created by | Purpose |
|---|---|---|
| `events`, `events_seq`, `aggregates`, `noop_events`, `events_to_publish`, functions `add_event(s)`, `add_undo_event`, `add_duplication_event` | `PostgresEventStoreState.initSchema()` | Event streams, duplication chains, undo marks, outbox; optimistic lock lives in the SQL functions |
| `event_bus`, `event_bus_seq` | `PostgresEventBusState.initSchema()` | Last-published-version cursor per aggregate |
| `events_to_route`, `events_to_route_seq` | `EventBusSchemaInitializer` (explicit) | Per-subscriber routing persistence |
| `subscriptions`, `subscriptions_seq` | `PostgresSubscriptionsState.initSchema()` | Per-projection per-aggregate progress cursor |
| `sagas` | `PostgresSagaState.initSchema()` | Saga progress (note: no PK/index — see CLAUDE.md §7) |
| `commands_responses` | `PostgresCommandResponseState.initSchema()` | Idempotent command responses |
| `types_names`, `types_names_seq` | `PostgresTypesNamesState.initSchema()` | Class name ↔ SMALLINT id mapping |
| `components_versions` | `PostgresVersionsState.initSchema()` | Aggregate/projection versions for replay decisions |
| `projection_<name>` (+ indices, auto-id `sequence_<name>`) | `PostgresDocumentStore*` constructor | JSONB read models |
| `aggregates_uids_seq`, `commands_uids_seq`, `sagas_uids_seq` | `PostgresUidGenerator` constructor | Id pools |

## Test / in-memory variants

`MemoryEventStoreState`, `MemoryEventBusState`, `MemorySubscriptionsState`,
`MemoryCommandResponseState`, `MemoryTypesNamesState`, `MemoryDocumentStore(AutoId)`,
`MemoryUidGenerator` — no DB needed; some batch/rare operations are unimplemented (`???`).
Intended for tests only (unbounded, non-durable).

## Pekko

No special Pekko config is required — plain `ActorSystem` (the framework does **not** use
pekko-persistence; the `postgres-journal` entries in `testdomain`'s `application.conf`
are vestigial). Set logging via `pekko.loglevel` and SLF4J/logback as usual.
