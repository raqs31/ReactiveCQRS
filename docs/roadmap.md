# Roadmap

Known gaps and planned work, carried over from the two `TODO` lists that used to live in
`README.md`. Items are as recorded by the authors; nothing here is scheduled or promised.

## Framework capabilities

- Asynchronous command handlers — pass an execution context to command handlers
  *(partially delivered: handlers can already return `AsyncCommandResult`, but the execution
  context is not injected — handlers currently supply their own, as in
  `examples/.../CommandHandlers.scala`)*
- Externalize the datastore — decouple the framework from the built-in PostgreSQL implementation
  *(the concrete blockers, for anyone attempting this, are catalogued in
  [operations.md](operations.md#can-i-use-another-database): four PL/pgSQL functions,
  `UPDATE ... RETURNING`, JSONB/GIN document storage, and Postgres catalog reads)*
- **`MemorySagaState` is missing from `core`.** Every other durable state has a `Memory*` variant,
  which makes it possible to run a whole system with no database — except that a system using sagas
  still needs PostgreSQL. The fulfilment example works around this with its own `InMemorySagaState`;
  promoting an equivalent into `core` would close the gap.
- Non-persistent projections
- Query for events and aggregate state
- Inject a clock into the framework, so time is controllable in tests and replays

## Projections and the event bus

- Projection rebuild
- Backpressure for projection rebuild
- Event bus database writes optimization — update aggregates in chunks
- Caching document store — to improve the performance of projection rebuilding
- Document store based on ScalikeJDBC — to improve query logging
- Common transaction for document stores

## Correctness and operations

- Handle `OptimisticLockingFailed`
- Verify and optimize database indices

## Known stale items in the tree

These are not roadmap items so much as things a newcomer will trip over:

- **`application.conf` references classes that do not exist.** Both
  `testdomain/src/main/resources/application.conf` and
  `testdomain/src/test/resources/application.conf` configure a Pekko Persistence journal and
  snapshot store pointing at `io.reactivecqrs.persistance.postgres.PostgresSyncJournal` and
  `...PostgresSyncSnapshotStore`. Neither class exists anywhere in the repository, and the
  framework does not use Pekko Persistence — it has its own event store. The configuration is
  inert leftover from an earlier design. Do not copy it into a new application; a fresh
  application needs no `application.conf` entries for ReactiveCQRS at all.
- **No aggregate snapshots.** An `AggregateRepositoryActor` rebuilds state by replaying the
  aggregate's entire event stream on first access. This is by design today, and it is the main
  reason to keep individual aggregates short-lived. See
  [operations.md](operations.md#aggregate-size).
- **`utils` module** exists in the tree but is not wired into `build.sbt`.

For a detailed, internals-level register of performance and integrity concerns — with
`file:line` references and severity estimates — see section 7 of `CLAUDE.md` in the repository
root.
