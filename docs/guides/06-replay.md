# Guide: replaying events and rebuilding projections

Because events are the system of record and read models are derived, any read model can be thrown
away and rebuilt from history. This is what makes it safe to change a projection's shape.

## When you need a rebuild

- A projection's document shape changed — new field, different structure.
- A projection had a bug and produced wrong data.
- You added a projection to a system that already has events.
- History was rewritten by a `RewriteHistoryCommand`, so read models no longer match the events.

## The mechanism

`EventsReplayerActor` reads the event store from the beginning and feeds events through
replay-mode repository actors into the event bus, so projections see the whole history as if it
were arriving live.

```scala
val replayerActor = system.actorOf(Props(new EventsReplayerActor(
  eventStoreState,
  eventBusActor,
  subscriptionsState,
  ReplayerConfig(),
  List(
    ReplayerRepositoryActorFactory(new BankAccountAggregateContext)
  ))))
```

You supply one `ReplayerRepositoryActorFactory` per aggregate type whose events should be
replayed — the same `AggregateContext` instances your command buses use.

### Running it

```scala
implicit val timeout: Timeout = Timeout(50.seconds)

val result: EventsReplayed = Await.result(
  (replayerActor ? ReplayAllEvents(
    batchPerAggregate = false,
    aggregatesTypes = Seq(AggregateType(classOf[BankAccount].getName)),
    delayBetweenAggregateTypes = 50)).mapTo[EventsReplayed],
  50.seconds)

println(s"replayed ${result.eventsCount} events")
```

| Parameter | Meaning |
|---|---|
| `batchPerAggregate` | Deliver all of an aggregate's events as one batch rather than individually. Faster; requires your listeners to handle batches, which `EventsListener` already does. |
| `aggregatesTypes` | Which aggregate types to replay. Use the fully-qualified aggregate root class name. |
| `delayBetweenAggregateTypes` | Milliseconds to pause between types — a crude throttle to keep a replay from saturating the database. |

Check the size of the job before starting one:

```scala
val status: ReplayerStatus = Await.result(
  (replayerActor ? GetStatus(Seq(AggregateType(classOf[BankAccount].getName)))).mapTo[ReplayerStatus],
  30.seconds)
// ReplayerStatus(willReplay, allEvents)
```

`ReplayerConfig` tunes the timeouts:

```scala
ReplayerConfig(maxReplayerInactivitySeconds = 30, replayerTimoutSeconds = 600)
```

A large replay will exceed the 600-second default. Raise it, and raise your `ask` timeout to match.

## Clearing before rebuilding

A replay re-delivers events; it does not empty your read model first. Two things must be in place
or you will layer new data on top of old:

**1. Implement `onClearProjectionData`.** It is called before a rebuild and must delete everything
this projection owns:

```scala
override protected def onClearProjectionData(): Unit =
  DB.autoCommit { implicit session =>
    sql"TRUNCATE TABLE projection_accounts_summary".update.apply()
  }
```

An empty implementation is the most common cause of a "rebuild" producing duplicated or
inconsistent data. The example module leaves it empty deliberately, with a comment — do not copy
that into a real projection.

**2. Bump the projection's `version`.** That is the signal that the read model is stale:

```scala
override protected val version: Int = 2   // was 1
```

The version is recorded per subscription in the `subscriptions` table. Alternatively send
`ClearProjectionData` to the projection actor directly and wait for `ProjectionDataCleared`.

## Rebuilding one projection out of many

Projections subscribe independently and track their own progress in `subscriptions`. Bumping one
projection's `version` marks only that one stale — the others keep their recorded position and
are not affected by the replay.

## Cost and impact

A replay is not free, and on a live system it competes with normal traffic:

- **Every event is read and deserialized.** Time scales with total event count, not with the size
  of the read model.
- **Projections do their full write work again.** For a document-store-backed projection that is
  one write per event (or per aggregate, with `batchPerAggregate`).
- **The event bus applies back-pressure**, so a slow projection throttles the replay rather than
  building an unbounded queue. That is protective, but it means one slow projection sets the pace.
- **There is no dedicated backpressure control for rebuilds** yet ([roadmap.md](../roadmap.md)).
  `delayBetweenAggregateTypes` is the only throttle exposed.

For a large system, replay during a maintenance window, or run a separate instance pointed at the
same database with only the projection being rebuilt registered.

## Practical sequence

1. Change the projection code and bump its `version`.
2. Make sure `onClearProjectionData` truncates everything it owns.
3. Deploy.
4. Run `ReplayAllEvents` for the aggregate types the projection listens to.
5. Verify the rebuilt read model before directing traffic at it.

## Related: aggregate replay

Aggregate state is also rebuilt from events, but that happens automatically and continuously —
every time an `AggregateRepositoryActor` starts, it replays that aggregate's entire stream. There
are no snapshots. You never trigger this; you only feel it as first-access latency on
long-streamed aggregates. See [../operations.md](../operations.md#aggregate-size).

## Next

- [../operations.md](../operations.md) — schema, indices, tuning, deployment
