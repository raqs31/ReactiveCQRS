# Comparison with other frameworks

Where ReactiveCQRS sits among event-sourcing tools, and — more usefully — where it does not.

> **On sourcing.** ReactiveCQRS's properties were verified against this repository. The other
> frameworks are characterised from their public documentation, located by search but not fetched
> directly in the environment where this page was written. Treat specifics about them as
> approximate and check the linked docs before making a decision; capabilities change between
> versions. Nothing here is a benchmark — no performance claims are made.

## At a glance

| | ReactiveCQRS | Pekko / Akka Persistence | Axon Framework | EventStoreDB (Kurrent) | Marten |
|---|---|---|---|---|---|
| Language | Scala | Scala / Java | Java / Kotlin | Any (client protocol) | .NET |
| Runtime model | Pekko actors | Pekko / Akka actors | POJO + Spring | Standalone database | Library over Postgres |
| Storage | PostgreSQL (fixed) | Pluggable journals | Pluggable / Axon Server | Purpose-built store | PostgreSQL |
| Snapshots | **No** | Yes | Yes | Yes | Yes |
| Clustering | **No** | Yes (Cluster Sharding) | Yes (Axon Server) | Yes | Via Postgres |
| Read models | Built in (`DocumentStore`) | Separate (Pekko/Akka Projection) | Built in | Subscriptions / projections | Built in |
| Sagas | Built in | Via Cluster Sharding / patterns | Built in | Not built in | Not built in |
| Time travel by version/instant | Built in | Via replay | Via replay | Built in | Built in |
| Undo as a first-class event | **Yes** | No | No | No | No |
| Copy-on-write aggregate duplication | **Yes** | No | No | No | No |
| Distribution | Private Nexus | Maven Central | Maven Central | Docker / binaries | NuGet |

## Pekko / Akka Persistence

The closest comparison, since ReactiveCQRS is built on Pekko actors and could in principle have
used Pekko Persistence instead.

**Where Pekko Persistence is stronger:**

- **Snapshots are built in**, so aggregate load time is bounded rather than proportional to
  history.
- **Cluster Sharding** distributes persistent actors across nodes while preserving the
  single-writer principle per `PersistenceId`. This is the capability ReactiveCQRS most visibly
  lacks.
- **Pluggable journals** — Cassandra, JDBC, R2DBC and others — rather than one fixed store.
- Much larger community, and it is on Maven Central.

**Where ReactiveCQRS offers more out of the box:**

- **Read models are part of the framework.** With Pekko you tag events and consume them via
  Pekko Projection, wiring the read side and its offset storage yourself. ReactiveCQRS ships
  `ProjectionActor` + `DocumentStore` with progress tracking in `subscriptions`.
- **Sagas are a first-class type** with persisted progress and a revert phase, rather than a
  pattern you assemble.
- **Querying an aggregate at a past version or instant** is a message you send
  (`GetAggregateForVersion`, `GetAggregateAtInstant`), not a replay you orchestrate.
- **Command flavors** encode the concurrency strategy in the type system.

The honest summary: Pekko Persistence is the better-engineered persistence layer; ReactiveCQRS is
a more complete, more opinionated application framework built on a narrower foundation.

Docs: [Pekko Persistence](https://pekko.apache.org/docs/pekko/1.3/typed/index-persistence.html),
[Pekko snapshotting](https://pekko.apache.org/docs/pekko/current/typed/persistence-snapshot.html),
[Pekko Projection](https://nightlies.apache.org/pekko/docs/pekko-projection/main-snapshot/docs/eventsourced.html),
[Akka Persistence](https://doc.akka.io/libraries/akka-core/current/typed/persistence.html).

## Axon Framework

The most feature-comparable framework, in the JVM world, to what ReactiveCQRS is trying to be —
CQRS, event sourcing, sagas and projections in one package.

**Where Axon is stronger:**

- **Snapshotting is configurable and automatic**, with a `Snapshotter` and a
  `SnapshotTriggerDefinition` that fires once an aggregate exceeds an event-count threshold. This
  is precisely the gap in ReactiveCQRS.
- **Upcasters** — a transformation chain between deserialization and application, so old event
  payloads can be migrated on read without keeping every historical class in your codebase.
  ReactiveCQRS's `eventsVersions` requires you to keep the old classes.
- Distributed deployment via Axon Server, annotation-driven programming model, Spring Boot
  integration, and a substantially larger ecosystem.

**Where ReactiveCQRS differs meaningfully:**

- Scala-native and functional — immutable case classes and partial functions rather than annotated
  mutable aggregates.
- Undo and duplication are supported by the store itself; in Axon you would model both by hand.
- No server component to run: one library plus your existing PostgreSQL.

Docs: [Axon event snapshots](https://docs.axoniq.io/axon-framework-reference/5.0/tuning/event-snapshots/),
[Axon repositories and event stores](https://legacy-docs.axoniq.io/reference-guide/v/2.4/repositories-and-event-stores.html),
[Baeldung on Axon snapshotting](https://www.baeldung.com/axon-snapshotting-aggregates).

## EventStoreDB / Kurrent

A different category — a purpose-built event-sourcing *database* rather than an application
framework.

**Where it is stronger:** a store designed for this workload, with subscriptions, built-in
projections, clustering and replication, and clients for many languages. If you have several
services in different languages sharing an event log, this is the shape that fits.

**Where ReactiveCQRS differs:** it is a framework, not a database — you get the aggregate,
command and saga programming model, but you must supply PostgreSQL and you get no clustering. If
you already run Postgres and want event sourcing inside one Scala application, adding a second
stateful system is a real cost that ReactiveCQRS avoids.

## Marten (.NET)

Worth a mention as the closest philosophical relative: a library that turns PostgreSQL into both a
document database and an event store, with projections and event versioning. Different ecosystem,
but the same "your existing Postgres is enough" bet — and it *does* have snapshots and inline/async
projection modes.

Docs: [Marten event versioning](https://martendb.io/events/versioning.html).

## When to use ReactiveCQRS

It fits well when most of these hold:

- You are writing **Scala 2.13** and already use Pekko.
- You **already run PostgreSQL** and would rather not add another stateful system.
- You want **CQRS, event sourcing, projections and sagas in one library** rather than assembling
  them.
- Your aggregates have **bounded lifecycles** — an order, a booking, an account with a
  natural end — so replay-on-load stays cheap.
- You are deploying a **single application instance**, or can confine projections to one node.
- **Undo, duplication, or "state as of version N" are domain features**, not just infrastructure
  niceties. This is where the framework is genuinely differentiated.

## When not to use ReactiveCQRS

Be blunt about the disqualifiers:

- **You need horizontal scale.** There is no clustering, and the in-memory caches in
  `PostgresDocumentStore` and `PostgresSubscriptionsState` have no cross-instance invalidation, so
  multiple nodes can serve stale reads. Use Pekko Persistence with Cluster Sharding.
- **You have long-lived aggregates.** No snapshots means load cost grows without bound, and
  `aggregateVersionLimit` (default 10 000) eventually stops you. Use Axon or Pekko Persistence.
- **You cannot use PostgreSQL.** The optimistic lock is PL/pgSQL; the store is not pluggable.
- **You need a non-JVM or polyglot event log.** Use EventStoreDB.
- **Dependency availability matters to you.** Artifacts are published to a private Nexus, not
  Maven Central, and the `mpjsons` serializer it depends on is likewise not on Central. If that
  host is unavailable, you cannot build. This is a supply-chain consideration, not a technical
  one, but it is the first thing that will block a new team.
- **You need a large community.** Realistically this is a single-maintainer project. Read
  section 7 of `CLAUDE.md` before committing — the known integrity and performance issues are
  documented there, and you would be maintaining around them yourself.

## The fair summary

ReactiveCQRS is a coherent, opinionated framework with two genuinely novel mechanisms — no-op-based
undo with `from_version` semantics, and copy-on-write aggregate duplication in the schema — that no
mainstream alternative offers. Its storage design is thoughtful: the optimistic lock and the outbox
insert happen in one PL/pgSQL call, which is a cleaner solution to the dual-write problem than many
larger frameworks manage.

Against that: no snapshots, no clustering, one supported database, private distribution, and a
documented list of known integrity issues. It is best understood as a well-designed framework for a
specific shape of application — single-instance, Postgres-backed, bounded-lifecycle aggregates,
where undo and duplication are worth building an architecture around — rather than as a general
alternative to Axon or Pekko Persistence.

## Next

- [background.md](background.md) — the patterns being implemented
- [design-rationale.md](design-rationale.md) — why the choices were made
