# Guide: projections

A projection turns the event stream into a query-optimised read model. It is a `ProjectionActor`
that listens to the event bus, writes documents to a store, and answers your query messages.

## Two rules before anything else

**Projections are eventually consistent.** A command returns as soon as its events are persisted;
delivery happens afterwards. Writing and then immediately reading the projection will often show
stale data. When you need read-your-writes, query the aggregate through the command bus, or use
`GetAggregateMinVersion` to wait for a specific version.

**Projections must be idempotent.** The event bus delivers at-least-once, can deliver out of
order, and a rebuild replays everything from the beginning. Applying the same event twice must not
corrupt the model. Prefer `overwriteDocument` (set to a computed value) over blind increments where
you can.

## The three listener types

| Listener | Receives | Use when |
|---|---|---|
| `AggregateListener` | `(id, version, created, Option[root])` | The read model is a function of *current state*. |
| `EventsListener` | `(id, Seq[EventInfo[root]])` | The read model is a function of *what happened* — counters, logs, time series. |
| `AggregateWithEventsListener` | `(id, version, Option[root], Seq[EventInfo[root]])` | You need both. |

A projection can register several listeners, including for different aggregate types — that is how
you build a read model spanning aggregates.

### Aggregate-based

The simplest. You mirror current state and never reason about which event did what, which also
makes idempotency nearly free.

```scala
class AccountsSummaryProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                val subscriptionsState: SubscriptionsState,
                                documentStore: DocumentStore[AccountSummary]) extends ProjectionActor {

  override protected val projectionName: String = "AccountsSummaryProjection"
  override protected val version: Int = 1

  protected val listeners = List(AggregateListener(accountChanged))

  private def accountChanged(id: AggregateId, aggregateVersion: AggregateVersion, created: Boolean,
                             account: Option[BankAccount]) = { implicit session: DBSession =>
    account match {
      case Some(a) if created => documentStore.insertDocument(0L, id.asLong, AccountSummary(a.owner, a.balance))
      case Some(a)            => documentStore.overwriteDocument(id.asLong, AccountSummary(a.owner, a.balance))
      case None               => documentStore.removeDocument(id.asLong)
    }
  }

  override protected def receiveQuery: Receive = {
    case GetAllAccounts => sender() ! documentStore.findAll().map { case (id, d) => AggregateId(id) -> d.document }
    case GetAccount(id) => sender() ! documentStore.getDocument(id.asLong).map(_.document)
  }

  override protected def onClearProjectionData(): Unit = { /* truncate this projection's table */ }
}
```

`created` distinguishes the first update from subsequent ones — insert vs overwrite. `None` means
the aggregate is deleted.

### Event-based

When the read model depends on what happened rather than on current state. The aggregate root is
not available.

```scala
protected val listeners = List(EventsListener(accountEvents))

private def accountEvents(id: AggregateId, events: Seq[EventInfo[BankAccount]]) = { implicit session: DBSession =>
  events.foreach(info => applyEvent(id, info))
}

private def applyEvent(id: AggregateId, info: EventInfo[BankAccount])(implicit session: DBSession): Unit =
  info.event match {
    case _: AccountOpened       => documentStore.insertDocument(0L, id.asLong, AccountActivity(0, 0, 0L))
    case MoneyDeposited(amount) => bump(id)(a => a.copy(deposits = a.deposits + 1, totalMoved = a.totalMoved + amount))
    case MoneyWithdrawn(amount) => bump(id)(a => a.copy(withdrawals = a.withdrawals + 1, totalMoved = a.totalMoved + amount))
    case _                      => ()   // events this read model does not care about
  }
```

Note the batch: several events for one aggregate can arrive together, each with its own `version`,
`userId` and `timestamp` in `EventInfo`. Iterate them in order.

Counters like the above are the classic idempotency hazard — a redelivered `MoneyDeposited` will
double-count. If exactness matters, either record the last processed version in the document and
skip anything at or below it, or use an `AggregateListener` and derive the value from state.

## Document stores

A document store is a Postgres table of JSONB documents, created automatically as
`projection_<tableName>`.

```scala
val store = new PostgresDocumentStore[AccountSummary]("accounts_summary", mpjsons, new NoopDocumentStoreCache)
```

Operations (all take an implicit `DBSession`, supplied by the listener callback):

```scala
store.insertDocument(spaceId, key, document)     // fails if the key exists
store.overwriteDocument(key, document)
store.updateDocument(spaceId, key, modify)       // read-modify-write
store.removeDocument(key)
store.getDocument(key)
store.getDocuments(keys)
store.findAll()
store.findDocumentByPath(Seq("owner"), "Alice")
store.findDocumentByPaths(ExpectedSingleTextValue(Seq("owner"), "Alice"),
                          ExpectedGreaterThanIntValue(Seq("balance"), 1000))
```

The key is a `Long` — usually `aggregateId.asLong`. `PostgresDocumentStoreAutoId` generates keys
from a sequence instead, for read models that are not one-per-aggregate.

`MemoryDocumentStore` exists for tests. Some of its batch insert overloads are unimplemented.

### Indices

Queries by JSON path scan unless you declare indices when constructing the store:

```scala
new PostgresDocumentStore[AccountSummary]("accounts_summary", mpjsons, new NoopDocumentStoreCache,
  indicies = Seq(
    MultipleTextIndex(1, Seq("owner")),
    UniqueLongIndex(2, Seq("externalRef"))))
```

The first argument of each index is a `uniqueId` that names the index — it must be stable and
unique within the store, since changing it creates a new index and orphans the old one. Available
kinds include `MultipleTextIndex`, `UniqueTextIndex`, `MultipleLongIndex`, `UniqueLongIndex`,
`MultipleIntIndex`, `UniqueIntIndex`, `MultipleTextArrayIndex`, and the combined
`UniqueCombinedIndex` / `MultipleCombinedIndex`.

### Caching

The third constructor argument is a `DocumentStoreCache`. `NoopDocumentStoreCache` disables
caching and is the safe default. A caching implementation speeds up rebuilds and hot reads, but
**there is no cross-instance invalidation** — with more than one application instance against one
database, a cache will serve stale data.

## Queries

`receiveQuery` runs on the projection actor's thread, so it is serialised with updates. Keep it
cheap: push filtering into the document store rather than loading everything and scanning in
Scala.

```scala
override protected def receiveQuery: Receive = {
  case GetAccountsByOwner(owner) =>
    sender() ! documentStore.findDocumentByPath(Seq("owner"), owner).values.map(_.document).toVector
}
```

Define the query messages yourself; there is no generic query protocol.

## Versioning and rebuilds

```scala
override protected val version: Int = 1
```

Bump `version` when the read model's shape changes. The projection compares it against the version
recorded in `subscriptions` and knows it is stale. `onClearProjectionData()` is called before a
rebuild — it must delete everything this projection owns:

```scala
override protected def onClearProjectionData(): Unit =
  DB.autoCommit { implicit session => sql"TRUNCATE TABLE projection_accounts_summary".update.apply() }
```

Leaving it empty means a rebuild layers new data on top of old. See
[06-replay.md](06-replay.md).

`projectionName` identifies the projection's subscription rows. Changing it makes the framework
treat it as a brand-new projection that has seen nothing.

## Tuning

`ProjectionActor` takes an optional `ProjectionActorOptions`:

```scala
class MyProjection(...) extends ProjectionActor(
  ProjectionActorOptions.DEFAULT
    .withGroupUpdatesDelayMillis(100)          // batch updates per aggregate before applying
    .withEventsToProcessImmediately(classOf[AccountOpened])  // exempt from the delay
    .withParallelUpdateProcessing(4)           // process updates on N threads
    .withEventsLogger(logger))
```

- `groupUpdatesDelayMillis` trades latency for throughput by batching bursts of events for the
  same aggregate. Zero (the default) applies each update as it arrives.
- `eventsToProcessImmediately` only applies to listeners that receive events; an
  `AggregateListener` has no event information to match on.
- `parallelUpdateProcessing` moves handling onto a thread pool. The handlers then run
  concurrently **on the same objects**, so your listener code has to be thread-safe. Leave it at
  zero unless you have measured a bottleneck.

## Subscribable projections

`SubscribableProjectionActor` lets clients subscribe to updates rather than poll:

```scala
class LiveAccountsProjection(...) extends SubscribableProjectionActor(updatesCacheTTL = Duration.ofSeconds(30)) {
  override protected def receiveSubscriptionRequest: Receive = {
    case SubscribeToAccount(id) =>
      handleSubscribe[AccountSummary, AccountUpdated]("account-" + id.asLong, sender(), ...)
  }
}
```

Clients receive `SubscriptionUpdated(subscriptionId, data)` and must renew with
`RenewSubscription`; idle subscriptions are cleared (default TTL 10 minutes). Cancel with
`CancelProjectionSubscriptions`.

## Registering

Projections must be constructed before the event bus starts publishing, and counted in the
subscriptions manager:

```scala
val expectedSubscribers = 2   // count your projections
val eventBusSubscriptionsManager =
  new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(expectedSubscribers))))

val summaryProjection = system.actorOf(
  Props(new AccountsSummaryProjection(eventBusSubscriptionsManager, subscriptionsState, summaryStore)),
  "AccountsSummaryProjection")
```

Set the count too high and the bus waits forever for a subscriber that never arrives — the
symptom is commands succeeding while every projection stays empty. Too low and projections can
miss early events.

There is currently no unsubscribe path; subscriptions only accumulate and are re-derived on
restart.

## Next

- [05-sagas.md](05-sagas.md) — coordinating across aggregates
- [06-replay.md](06-replay.md) — rebuilding a projection
