# Guide: events

Events are the durable record. Everything else — aggregate state, projections, reports — is
derived and can be thrown away and rebuilt. Events cannot.

## The five kinds

### `Event` — an ordinary fact

```scala
case class MoneyDeposited(amount: Long) extends Event[BankAccount]
```

Applied by your event handler to produce new state. This is 95% of events.

### `FirstEvent` — creates the aggregate

```scala
case class AccountOpened(owner: String, initialBalance: Long) extends FirstEvent[BankAccount] {
  override def spaceId: SpaceId = SpaceId(0)
}
```

Exactly one event type per aggregate should be a `FirstEvent` (or a `DuplicationEvent`). It must
supply a **`SpaceId`**, a coarse partitioning key — tenant, workspace, customer. It is written to
the `aggregates` row at creation and is immutable thereafter; changing it later requires rewriting
history. If you have no partitioning scheme, `SpaceId(0)` is the conventional "everything" space,
but think about whether you will want tenancy before you have a million aggregates in space zero.

### `UndoEvent` — cancel the previous *n* events

```scala
case class LastOperationsUndone(eventsCount: Int) extends UndoEvent[BankAccount]
```

Emitted like any other event:

```scala
def undoLastOperations(command: UndoLastOperations): CommandResult =
  CommandSuccess(LastOperationsUndone(command.stepsToUndo))
```

The framework marks the cancelled events as no-ops in the `noop_events` table and rebuilds the
aggregate without them. **You do not write reversal logic** — your event handler for the undo event
usually just returns state unchanged:

```scala
case e: LastOperationsUndone => account
```

It still needs a case, or you get a `MatchError`.

The cancelled events remain in the `events` table; they are excluded during replay. So undo is
itself auditable, and an undo can be undone.

### `DuplicationEvent` — copy an aggregate from a point in its history

```scala
case class AccountCloned(spaceId: SpaceId, baseAggregateId: AggregateId, baseAggregateVersion: AggregateVersion)
  extends DuplicationEvent[BankAccount]
```

`DuplicationEvent` extends `FirstEvent`, so it creates a new aggregate — but instead of starting
empty, the new aggregate shares the base aggregate's history up to `baseAggregateVersion` and
diverges from there. The `aggregates` table models this with `base_id` / `base_order` /
`base_version` columns (a "duplication chain").

The event handler typically returns state unchanged, because the state comes from the replayed
base history:

```scala
case e: AccountCloned => account
```

Useful for templates, "save as copy", and what-if scenarios. Note the shared history is not
copied, so duplication is cheap in storage but the chain is walked on load.

### `PermanentDeleteEvent` — erase

```scala
case class AccountPurged() extends PermanentDeleteEvent[BankAccount]
```

Hard-deletes the aggregate **and its events**. This is irreversible and destroys the audit trail;
it exists for erasure obligations. For ordinary "the user deleted this", emit a normal event whose
handler returns `null` — the aggregate reads as deleted while history is preserved:

```scala
case class AccountClosed() extends Event[BankAccount]
// ...
case e: AccountClosed => null
```

## Designing events

**Record what happened, not the resulting state.** `MoneyDeposited(500)` survives a change in how
balances are computed; `BalanceChangedTo(10500)` bakes today's computation into permanent storage.

**Name in the past tense.** `MoneyWithdrawn`, not `WithdrawMoney` — that is the command.

**Keep them flat and explicit.** Events are stored as JSON keyed by class name. Nesting domain
objects inside events couples the stored format to classes you will want to refactor. Prefer
primitives and small value types.

**Include what a projection will need.** A projection built from `EventsListener` sees only the
event, not the aggregate. If a read model needs the account owner on every deposit row, the event
has to carry it — or the projection has to use an `AggregateListener` instead.

**Do not put derived data in events.** If it can be computed from other events, computing it keeps
the record smaller and avoids two sources of truth disagreeing after a bug fix.

## Evolving an event

This is the part that bites in production. Events already written to `events` are JSON that must
still deserialize years later, keyed by `event_type_id` and `event_type_version`.

**Safe changes:**
- Adding a field with a default — old JSON deserializes with the default.
- Adding a new event type.

**Breaking changes:**
- Renaming the class.
- Renaming or removing a field.
- Changing a field's type.

For a breaking change, keep the old class and register both in `eventsVersions`:

```scala
// The original, kept forever so old rows still deserialize
case class AccountOpenedV0(owner: String, initialBalance: Long) extends FirstEvent[BankAccount] {
  override def spaceId: SpaceId = SpaceId(0)
}

// The new shape
case class AccountOpened(owner: String, initialBalance: Long, currency: String) extends FirstEvent[BankAccount] {
  override def spaceId: SpaceId = SpaceId(0)
}
```

```scala
override val eventsVersions: List[EventVersion[BankAccount]] =
  EV[AccountOpened](
    0 -> classOf[AccountOpenedV0],
    1 -> classOf[AccountOpened]) ::
  Nil
```

`EV[Base](version -> class, ...)` maps a stored version number to the class that can read it. New
events are written at the highest registered version; old rows deserialize into the old class.

Your event handler then needs cases for both:

```scala
case e: AccountOpenedV0 => BankAccount(e.owner, e.initialBalance, "EUR")  // sensible default
case e: AccountOpened   => BankAccount(e.owner, e.initialBalance, e.currency)
```

The alternative — a `RewriteHistoryCommand` that rewrites the old events into the new shape — is
available ([02-commands.md](02-commands.md#rewritehistorycommand--change-the-past)) but destroys
the original record. Prefer versioning.

If you change an event class **without** registering a version, replay fails at deserialization,
and it fails for every aggregate that ever emitted that event. The failure surfaces the first time
an affected aggregate is loaded, which may be long after deployment.

## Events in the event handler

Signature reminder:

```scala
override def eventHandlers = (userId, timestamp, account) => {
  case e: MoneyDeposited => account.copy(balance = account.balance + e.amount)
}
```

`userId` and `timestamp` come from the persisted event row, not the current clock. That is
deliberate: replaying history must produce the same state it produced originally. Never call
`Instant.now()` in an event handler.

Every event type needs a case, including ones that do not change state (`UndoEvent`,
`DuplicationEvent`, and typically any event that exists purely to be picked up by a projection).

## What events are stored with

| Column | Meaning |
|---|---|
| `id` | Global event sequence number. |
| `aggregate_id`, `version` | Which aggregate, and its version after this event. |
| `user_id` | Who caused it — from the command. |
| `event_time` | When it was persisted. |
| `event_type_id`, `event_type_version` | Resolve back to a class via `eventsVersions`. |
| `event` | The JSON payload. |

There is a maximum event size; exceeding it raises `EventTooLargeException`. An aggregate
exceeding its version limit (default 10 000) raises `TooManyEventsException`. Both are signals
that an aggregate boundary is wrong.

## Next

- [04-projections.md](04-projections.md) — turning events into read models
- [06-replay.md](06-replay.md) — rebuilding read models from history
