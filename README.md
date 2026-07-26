# ReactiveCQRS

A Scala library for building applications with **CQRS** and **Event Sourcing** on top of
[Apache Pekko](https://pekko.apache.org/) actors, with PostgreSQL as the durable store.

Writes go through commands that produce immutable events; those events are the system of record.
Read models ("projections") are derived from the event stream and kept up to date asynchronously.
Because the events are kept forever, you can query any aggregate at any past version or instant,
undo changes, duplicate an aggregate from a point in its history, and rebuild read models from
scratch whenever their shape changes.

```scala
// Decide
case class Withdraw(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, amount: Long)
  extends ConcurrentCommand[BankAccount, CustomCommandResponse[_]]

def withdraw(account: BankAccount, command: Withdraw): CommandResult =
  if (account.balance < command.amount) CommandFailure("Insufficient funds")
  else                                  CommandSuccess(MoneyWithdrawn(command.amount))

// Apply
def moneyWithdrawn(account: BankAccount, event: MoneyWithdrawn): BankAccount =
  account.copy(balance = account.balance - event.amount)
```

> **Note on Akka vs Pekko.** This project migrated from Akka to Apache Pekko. All imports are
> `org.apache.pekko.*`; do not add `akka` dependencies.

## Requirements

- Scala 2.13 (built against 2.13.18, targeting JVM 1.8 bytecode)
- JDK 8 or newer
- PostgreSQL — the event store, event bus, saga state and document stores are all Postgres-backed
- sbt 1.x

## Installing

Artifacts are published to a private Nexus, **not** to Maven Central:

```scala
resolvers += "neula" at "https://nexus.neula.in/repository/neula-releases"

libraryDependencies ++= Seq(
  "io.reactivecqrs" %% "reactivecqrs-api"  % "0.12.46",
  "io.reactivecqrs" %% "reactivecqrs-core" % "0.12.46"
)
```

`reactivecqrs-api` alone is enough for a module that only defines domain types; `reactivecqrs-core`
brings the actors, stores and the PostgreSQL/ScalikeJDBC dependency.

### One thing that will bite you

ReactiveCQRS uses the **default singleton ScalikeJDBC connection pool** and does not manage it.
Your application must initialise the pool *before* constructing anything from the framework:

```scala
Class.forName("org.postgresql.Driver")
ConnectionPool.singleton(jdbcUrl, user, password, ConnectionPoolSettings(initialSize = 5, maxSize = 20))
```

Schema creation is then automatic and idempotent — every `initSchema()` is `CREATE ... IF NOT EXISTS`.

## Documentation

| Document | What it covers |
|---|---|
| [Getting started](docs/getting-started.md) | Database setup, wiring a system, first command, reading it back |
| [Concepts](docs/concepts.md) | Aggregates, events, commands, versions, spaces, consistency model |
| [Architecture](docs/architecture.md) | The actor pipeline, write path, read path, event delivery |
| [Guide: aggregates](docs/guides/01-aggregates.md) | Defining a domain: root, events, handlers, `AggregateContext` |
| [Guide: commands](docs/guides/02-commands.md) | Command flavors, concurrency, failures, async, idempotency |
| [Guide: events](docs/guides/03-events.md) | Event kinds, undo, duplication, deletion, schema evolution |
| [Guide: projections](docs/guides/04-projections.md) | Read models, listener types, document stores, ordering |
| [Guide: sagas](docs/guides/05-sagas.md) | Multi-aggregate processes, compensation, resumption |
| [Guide: replay](docs/guides/06-replay.md) | Rebuilding projections from history |
| [Operations](docs/operations.md) | Schema reference, indices, tuning, deployment caveats |
| [Roadmap](docs/roadmap.md) | Known gaps and planned work |

Background reading, for deciding whether this is the right tool and understanding why it works the
way it does:

| Document | What it covers |
|---|---|
| [Background](docs/background.md) | CQRS and Event Sourcing fundamentals, with a reading list, and how the standard vocabulary maps onto this library |
| [Design rationale](docs/design-rationale.md) | Why the design is what it is — the stored-procedure lock, the absence of snapshots, undo via no-op events, duplication chains |
| [Comparison](docs/comparison.md) | Honest positioning against Pekko/Akka Persistence, Axon, EventStoreDB and Marten, including when *not* to use this |

`CLAUDE.md` in the repository root is a separate, internals-focused map aimed at contributors and
automated tooling; it also carries a register of known performance and integrity hotspots.

## Modules

| Module | Purpose |
|---|---|
| `api` | Public domain API — `Aggregate`, `Event`, `Command`, results, ids. Depends only on Pekko. |
| `core` | The machinery — actors, event store, event bus, projections, sagas, document store, id generator. |
| `examples` | A runnable bank-account example that the documentation is written against. Not published. |
| `testdomain` | Shopping-cart domain used by the integration specs. |
| `testutils` | `CommonSpec` and actor-ask helpers for tests. |

## Building

```bash
sbt compile                                      # all modules
sbt test                                         # all tests (Postgres-backed ones need a database)
sbt core/test
sbt "testdomain/testOnly *ReactiveTestDomainSpec"
sbt "examples/runMain io.reactivecqrs.example.bank.BankExampleApp"
```

The library version lives in `project/Common.scala`, not in the per-module `build.sbt` files.

## License

MIT — see [LICENSE](LICENSE).
