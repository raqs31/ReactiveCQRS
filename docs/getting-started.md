# Getting started

By the end of this page you will have a running ReactiveCQRS system, will have issued a command
that created an aggregate, and will have read that aggregate back — both its current state and an
earlier version of it.

Everything here corresponds to real code in the [`examples`](../examples) module. If you would
rather read the finished thing first, start at
[`BankExampleApp.scala`](../examples/src/main/scala/io/reactivecqrs/example/bank/BankExampleApp.scala).

## 1. A database

> **In a hurry, or just exploring?** You can skip this section entirely. The framework has
> memory-backed implementations of every durable state, so a complete system runs with no database:
>
> ```bash
> sbt "examples/runMain io.reactivecqrs.example.fulfilment.FulfilmentApp --in-memory"
> ```
>
> See [Running without a database](#running-without-a-database) below. Come back here before you
> build anything real — in-memory mode skips serialization entirely, so it will not catch an event
> shape that cannot be stored.

ReactiveCQRS needs PostgreSQL. It creates its own tables, sequences and stored procedures, so an
empty database and a user who owns it is all you have to provide.

```sql
CREATE USER reactivecqrs WITH PASSWORD 'reactivecqrs';
CREATE DATABASE reactivecqrs OWNER reactivecqrs;
```

Or with Docker:

```bash
docker run -d --name reactivecqrs-pg -p 5432:5432 \
  -e POSTGRES_USER=reactivecqrs \
  -e POSTGRES_PASSWORD=reactivecqrs \
  -e POSTGRES_DB=reactivecqrs \
  postgres:16
```

The user needs `CREATE` on the database — the framework issues `CREATE TABLE IF NOT EXISTS`,
`CREATE SEQUENCE IF NOT EXISTS` and `CREATE OR REPLACE FUNCTION` on startup. Owning the database
covers this.

## 2. Dependencies

```scala
resolvers += "neula" at "https://nexus.neula.in/repository/neula-releases"

libraryDependencies ++= Seq(
  "io.reactivecqrs" %% "reactivecqrs-api"  % "0.12.46",
  "io.reactivecqrs" %% "reactivecqrs-core" % "0.12.46",
  "org.postgresql"   % "postgresql"        % "42.7.10"
)
```

You do **not** need any `application.conf` entries. (If you copy configuration from the
`testdomain` module, note that its `application.conf` configures a Pekko Persistence journal that
this framework does not use and whose classes no longer exist — see
[roadmap.md](roadmap.md#known-stale-items-in-the-tree).)

## 3. Initialise the connection pool first

This is the single most common setup mistake. The framework uses ScalikeJDBC's **default
singleton pool** (`DB.autoCommit`, `DB.readOnly`, `DB.localTx`) and never creates it. Several
framework constructors talk to the database immediately, so the pool has to exist before you
build anything:

```scala
Class.forName("org.postgresql.Driver")
ConnectionPool.singleton(
  "jdbc:postgresql://localhost:5432/reactivecqrs", "reactivecqrs", "reactivecqrs",
  ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L))
```

If you get `java.lang.IllegalStateException: Connection pool is not yet initialized` from
somewhere deep in a `*State` constructor, this is why.

## 4. Define a domain

The smallest useful domain is an aggregate root, one event, one command, and an
`AggregateContext` binding them together.

```scala
case class BankAccount(owner: String, balance: Long)

case class AccountOpened(owner: String, initialBalance: Long) extends FirstEvent[BankAccount] {
  override def spaceId: SpaceId = SpaceId(0)
}

case class OpenAccount(idempotencyId: Option[SagaStep], userId: UserId, owner: String, initialBalance: Long)
  extends FirstCommand[BankAccount, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

class BankAccountAggregateContext extends AggregateContext[BankAccount] {
  override val version: Int = 1

  override def commandHandlers = account => {
    case c: OpenAccount => CommandSuccess(AccountOpened(c.owner, c.initialBalance))
  }

  override def eventHandlers = (userId, timestamp, account) => {
    case e: AccountOpened => BankAccount(e.owner, e.initialBalance)
  }

  override def initialAggregateRoot: BankAccount = BankAccount("", 0L)
}
```

Three things worth noticing:

- **A command handler returns events, it does not mutate anything.** Persisting is the
  framework's job.
- **An event handler is a pure state transition.** It runs on every replay, so it must not
  validate, throw, or reach outside itself.
- **`FirstEvent` requires a `SpaceId`.** It is a coarse partitioning key (tenant, workspace).
  `SpaceId(0)` is fine if you have no partitioning scheme; it is fixed at creation and cannot be
  changed later.

The full version, with every command flavor and every event kind, is in
[`examples/.../bank`](../examples/src/main/scala/io/reactivecqrs/example/bank). See
[guides/01-aggregates.md](guides/01-aggregates.md) for the reasoning behind each piece.

## 5. Wire the system

There is no container and no auto-configuration; you assemble the actors yourself. The order
matters — pool, then serialization, then stores, then actors.

```scala
val system = ActorSystem("bank-example")

// Serialization and the shared type-name registry
val mpjsons = new MPJsons
val typesNamesState = new PostgresTypesNamesState().initSchema()

// Durable state. Every initSchema() is idempotent; call them on every boot.
val eventStoreState      = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
val commandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
val eventBusState        = new PostgresEventBusState().initSchema()
val subscriptionsState   = new PostgresSubscriptionsState(typesNamesState, keepInMemory = true).initSchema()

// Id generation — three Postgres sequences, each handing out pools of ids
val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
  new PostgresUidGenerator("aggregates_uids_seq"),
  new PostgresUidGenerator("commands_uids_seq"),
  new PostgresUidGenerator("sagas_uids_seq"))), "uidGenerator")

// Event bus. The argument is how many subscribers to expect — count your projections.
val eventBusSubscriptionsManager =
  new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(1))))
val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState, eventBusSubscriptionsManager)), "eventBus")

// One command bus per aggregate type
val accountsCommandBus = system.actorOf(
  AggregateCommandBusActor(new BankAccountAggregateContext, uidGenerator, eventStoreState,
    commandResponseState, eventBusActor, eventsReplayMode = false),
  "BankAccountCommandBus")
```

The `EventBusSubscriptionsManager(n)` argument deserves attention: it is the number of
subscribers the bus waits for before it starts publishing. Set it too high and events sit
undelivered waiting for a subscriber that never registers; too low and projections can miss the
earliest events. Count your `ProjectionActor`s.

## 6. Issue a command

Commands are ordinary messages. The response is a value, not an exception — a rejected command
comes back as `FailureResponse`.

```scala
implicit val timeout: Timeout = Timeout(30.seconds)

val response = Await.result(
  (accountsCommandBus ? OpenAccount(None, UserId(1L), "Alice", 10000L)).mapTo[CustomCommandResponse[_]],
  30.seconds)

val accountId = response.asInstanceOf[SuccessResponse].aggregateId
```

A `FirstCommand` has no `aggregateId` — the bus allocates one and hands it back in
`SuccessResponse(aggregateId, aggregateVersion)`. Every subsequent command for that account
carries the id.

`Await` is used here to keep the example linear. In real code, keep the `Future` and compose it;
and never block inside an actor.

## 7. Read it back

State queries also go through the command bus, which routes them to the aggregate's repository
actor. They answer with a `Try`, because asking for an aggregate that does not exist is a failure,
not an empty result.

```scala
// Current state
val current: Try[Aggregate[BankAccount]] =
  Await.result((accountsCommandBus ? GetAggregate(accountId)).mapTo[Try[Aggregate[BankAccount]]], 30.seconds)

current.get.aggregateRoot  // Some(BankAccount("Alice", 10000))
current.get.version        // Version(1)

// State as of an earlier version — this is the event sourcing payoff
val old: Try[Aggregate[BankAccount]] =
  Await.result(
    (accountsCommandBus ? GetAggregateForVersion(accountId, AggregateVersion(1))).mapTo[Try[Aggregate[BankAccount]]],
    30.seconds)
```

The available queries are `GetAggregate`, `GetAggregateForVersion`, `GetAggregateAtInstant`,
`GetAggregateMinVersion` (which waits, up to a timeout, until the aggregate reaches a version),
`GetEventsForAggregate` and `GetEventsForAggregateForVersion`.

These read the **write side** and are strongly consistent. Projections are a different story —
see below.

## 8. Add a read model

Projections turn the event stream into query-optimised documents. The simplest kind mirrors
current aggregate state:

```scala
class AccountsSummaryProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                val subscriptionsState: SubscriptionsState,
                                documentStore: DocumentStore[AccountSummary]) extends ProjectionActor {

  override protected val projectionName: String = "AccountsSummaryProjection"
  override protected val version: Int = 1

  protected val listeners = List(AggregateListener(accountChanged))

  private def accountChanged(id: AggregateId, v: AggregateVersion, created: Boolean,
                             account: Option[BankAccount]) = { implicit session: DBSession =>
    account match {
      case Some(a) if created => documentStore.insertDocument(0L, id.asLong, AccountSummary(a.owner, a.balance))
      case Some(a)            => documentStore.overwriteDocument(id.asLong, AccountSummary(a.owner, a.balance))
      case None               => documentStore.removeDocument(id.asLong)
    }
  }

  override protected def receiveQuery: Receive = {
    case GetAllAccounts => sender() ! documentStore.findAll().map { case (id, d) => AggregateId(id) -> d.document }
  }

  override protected def onClearProjectionData(): Unit = { /* truncate this projection's table */ }
}
```

Register it before the command bus starts publishing, and remember to include it in the
`EventBusSubscriptionsManager` count:

```scala
val store = new PostgresDocumentStore[AccountSummary]("accounts_summary", mpjsons, new NoopDocumentStoreCache)
val projection = system.actorOf(
  Props(new AccountsSummaryProjection(eventBusSubscriptionsManager, subscriptionsState, store)),
  "AccountsSummaryProjection")
```

**Projections are eventually consistent.** A command returns as soon as its events are persisted;
delivery to projections happens afterwards. If you write and then immediately query the
projection, you will often see stale data — that is not a bug. When you need read-your-writes,
query the aggregate through the command bus instead. See
[guides/04-projections.md](guides/04-projections.md).

## 9. Run the example

```bash
sbt "examples/runMain io.reactivecqrs.example.bank.BankExampleApp"
```

Override the connection if your database is elsewhere:

```bash
sbt -Dbank.jdbcUrl=jdbc:postgresql://localhost:5433/mydb -Dbank.dbUser=me -Dbank.dbPassword=secret \
  "examples/runMain io.reactivecqrs.example.bank.BankExampleApp"
```

It opens two accounts, deposits, renames, demonstrates a rejected withdrawal, runs a money
transfer saga across both accounts, reads an old version, and prints both projections.

## Running without a database

`core` ships a memory-backed implementation of every durable state, so the whole pipeline —
commands, events, the event bus, projections, sagas — runs with no PostgreSQL at all. This is
useful for exploring the programming model and for unit tests that would otherwise need a database.

The wiring is identical except for the constructor calls, because every actor depends on the
`*State` abstraction rather than on its implementation:

| Component | Memory | PostgreSQL |
|---|---|---|
| Event store | `MemoryEventStoreState` | `PostgresEventStoreState` |
| Event bus cursor | `MemoryEventBusState` | `PostgresEventBusState` |
| Subscriptions | `MemorySubscriptionsState` | `PostgresSubscriptionsState` |
| Command responses | `MemoryCommandResponseState` | `PostgresCommandResponseState` |
| Type names | `MemoryTypesNamesState` *(exists, but nothing in the memory path needs it — the memory states do not map class names to ids)* | `PostgresTypesNamesState` |
| Id generation | `MemoryUidGenerator` | `PostgresUidGenerator` |
| Document stores | `MemoryDocumentStore` | `PostgresDocumentStore` |
| Saga state | — **not shipped** | `PostgresSagaState` |

```scala
val system = ActorSystem("in-memory-example")

val eventStoreState      = new MemoryEventStoreState
val commandResponseState = new MemoryCommandResponseState
val eventBusState        = new MemoryEventBusState
val subscriptionsState   = new MemorySubscriptionsState

val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
  new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)), "uidGenerator")

// ...and from here the wiring is exactly as in section 5 — no initSchema() calls, no pool.
val store: DocumentStore[AccountSummary] = new MemoryDocumentStore[AccountSummary]
```

No `ConnectionPool.singleton(...)` and no `initSchema()` calls: there is no schema.

**The one gap is `SagaState`.** Every other state has a `Memory*` variant, but sagas do not, so a
system using them still needs PostgreSQL unless you supply your own. The fulfilment sample does
exactly that — see
[`InMemorySagaState`](../examples/src/main/scala/io/reactivecqrs/example/fulfilment/InMemorySagaState.scala),
which is about forty lines and mirrors the Postgres semantics. Promoting an equivalent into `core`
is on the [roadmap](roadmap.md).

### What in-memory mode will not tell you

Use it to learn the API and to test domain logic — not to validate that a design will work in
production:

- **Nothing is serialized.** PostgreSQL mode round-trips every event, saga order and read-model
  document through mpjsons. In memory these are plain object references, so an event shape mpjsons
  cannot handle passes in memory and fails against a database. This is the difference most likely
  to bite you.
- **Nothing survives a restart**, so saga crash-resumption — the entire reason saga progress is
  persisted — cannot be exercised.
- **The real optimistic lock is not involved.** The PostgreSQL write path takes a row lock inside
  the `add_event` stored procedure; the memory store does not reproduce it, so contention behaves
  differently.
- **`UndoEvent`, `DuplicationEvent` and permanent delete** rely on `noop_events` and duplication
  chains in the schema. `MemoryEventStoreState` also leaves `overwriteEvents` (history rewrite) and
  `countEventsForAggregateTypes` unimplemented.

Run against PostgreSQL at least once before trusting your event schema.

For why H2 or another database is not an option, see
[operations.md](operations.md#can-i-use-another-database).

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Connection pool is not yet initialized` | `ConnectionPool.singleton(...)` was not called, or was called after a framework object was constructed. |
| Commands succeed but projections stay empty | The `EventBusSubscriptionsManager(n)` count is higher than the number of projections that actually registered, so the bus is still waiting. |
| `MatchError` on a command or event | `commandHandlers` / `eventHandlers` are `PartialFunction`s and are not checked at compile time. Every command and every event type needs a case. |
| `AggregateConcurrentModificationError` | A strict `Command`'s `expectedVersion` did not match. Re-read the aggregate and retry, or use `ConcurrentCommand`, which retries for you. |
| Deserialization errors after changing an event class | The stored JSON no longer matches the class. Register a mapping in `eventsVersions` — see [guides/03-events.md](guides/03-events.md#evolving-an-event). |
| First access to an old aggregate is slow | There are no snapshots; the whole event stream is replayed. See [operations.md](operations.md#aggregate-size). |

## Where to next

- [Concepts](concepts.md) — the vocabulary and the consistency model
- [Architecture](architecture.md) — what actually happens when you send a command
- [Guides](guides/01-aggregates.md) — one topic at a time
