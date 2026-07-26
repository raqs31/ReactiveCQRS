# Guide: defining an aggregate

An aggregate is the unit of consistency. This guide walks through defining one, and through the
decisions that are hard to reverse later.

All code here is from
[`examples/.../bank`](../../examples/src/main/scala/io/reactivecqrs/example/bank).

## The five pieces

| Piece | File in the example |
|---|---|
| The aggregate root | `Model.scala` |
| Events | `Events.scala` |
| Commands | `Commands.scala` |
| Handlers | `CommandHandlers.scala`, `EventHandlers.scala` |
| The binding | `BankAccountAggregateContext.scala` |

Splitting handlers into their own objects is a convention, not a requirement — it keeps the
context readable once you have more than a handful of cases.

## 1. The aggregate root

A plain immutable case class. No framework types, no annotations, no base class.

```scala
case class BankAccount(owner: String, balance: Long)
```

Keep it small. This object is rebuilt from scratch every time the aggregate is loaded, held in
memory for as long as the aggregate is active, and serialised into every projection update. It
should hold exactly the state your command handlers need to make decisions — nothing that exists
only for display, which belongs in a projection.

## 2. Events

Past tense, one per meaningful state change.

```scala
case class AccountOpened(owner: String, initialBalance: Long) extends FirstEvent[BankAccount] {
  override def spaceId: SpaceId = SpaceId(0)
}

case class MoneyDeposited(amount: Long) extends Event[BankAccount]
case class MoneyWithdrawn(amount: Long) extends Event[BankAccount]
case class OwnerRenamed(owner: String)  extends Event[BankAccount]
```

Exactly one event type should extend `FirstEvent` (or `DuplicationEvent`) — that is the one that
brings the aggregate into existence, and it is where the `SpaceId` is fixed forever.

Events are stored as JSON and kept permanently. Design them as a published API: prefer explicit
fields over nested domain objects that you might refactor, and record *what happened* rather than
*what the new state is*. `MoneyDeposited(amount)` survives a change in how balances are computed;
`BalanceChangedTo(newBalance)` does not.

See [03-events.md](03-events.md) for the other event kinds and for schema evolution.

## 3. Commands

Imperative, and always rejectable.

```scala
case class OpenAccount(idempotencyId: Option[SagaStep], userId: UserId, owner: String, initialBalance: Long)
  extends FirstCommand[BankAccount, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

case class RenameOwner(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, owner: String)
  extends Command[BankAccount, CustomCommandResponse[_]]

case class Deposit(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, amount: Long)
  extends ConcurrentCommand[BankAccount, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]
```

The base class you extend is a routing decision, covered in [02-commands.md](02-commands.md).
Briefly: `FirstCommand` creates, `Command` asserts a version, `ConcurrentCommand` retries.

`userId` is required on every command. It is stored on each event, so the event stream doubles as
an audit log.

## 4. Command handlers — the decisions

```scala
def withdraw(account: BankAccount, command: Withdraw): CommandResult =
  if (command.amount <= 0) {
    CommandFailure("Withdrawal amount must be positive")
  } else if (account.balance < command.amount) {
    CommandFailure(s"Insufficient funds: balance is ${account.balance}, requested ${command.amount}")
  } else {
    CommandSuccess(MoneyWithdrawn(command.amount))
  }
```

Rules:

- **All validation lives here.** Once an event exists it is a fact; there is no later opportunity
  to refuse it.
- **No side effects.** Returning an event *is* the effect. Do not write to a database, call a
  service, or send a message from a handler.
- **Failure is a value.** `CommandFailure` comes back to the caller as `FailureResponse`. Throwing
  works, but produces an opaque `CommandHandlingError` with an error id instead of a useful
  message.
- **Several events at once are fine.** `CommandSuccess(Seq(e1, e2))` persists both in one
  transaction; the version increments once per event.
- **Never block.** For I/O, return an `AsyncCommandResult` — see
  [02-commands.md](02-commands.md#asynchronous-handlers).

## 5. Event handlers — the state transitions

```scala
def moneyWithdrawn(account: BankAccount, event: MoneyWithdrawn): BankAccount =
  account.copy(balance = account.balance - event.amount)
```

Rules, and these are stricter than they look:

- **Total.** Never throw, never validate, never reject. The event already happened.
- **Pure.** No I/O, no `Instant.now()`, no randomness, no counters. These handlers re-run on every
  replay, and a replay that produces different state than the original run means your history no
  longer reconstructs your present.
- **Return `null` to delete.** The framework stores that as `None`.

If you need the time or the acting user, they are passed in — from the stored event row, not the
current clock:

```scala
override def eventHandlers = (userId, timestamp, account) => {
  case e: MoneyDeposited => account.copy(balance = account.balance + e.amount, lastTouchedBy = userId)
}
```

## 6. The `AggregateContext`

The object that binds it all together, and the only thing you hand to the framework.

```scala
class BankAccountAggregateContext extends AggregateContext[BankAccount] {

  override val version: Int = 1

  override val eventsVersions: List[EventVersion[BankAccount]] =
    EV[AccountOpened](0 -> classOf[AccountOpened]) ::
    EV[OwnerRenamed](0  -> classOf[OwnerRenamed]) ::
    Nil

  override def commandHandlers = account => {
    case c: OpenAccount        => openAccount(c)
    case c: Deposit            => deposit(c)
    case c: Withdraw           => withdraw(account, c)
    case c: RenameOwner        => renameOwner(c)
    case c: UndoLastOperations => undoLastOperations(c)
    case c: CloseAccount       => closeAccount(account, c)
    case c: PurgeAccount       => purgeAccount(c)
  }

  override def eventHandlers = (userId, timestamp, account) => {
    case e: AccountOpened        => accountOpened(e)
    case e: MoneyDeposited       => moneyDeposited(account, e)
    case e: MoneyWithdrawn       => moneyWithdrawn(account, e)
    case e: OwnerRenamed         => ownerRenamed(account, e)
    case e: LastOperationsUndone => account
    case e: AccountClosed        => null
    case e: AccountPurged        => null
  }

  override def initialAggregateRoot: BankAccount = BankAccount("", 0L)
}
```

Four things to be careful about:

**`commandHandlers` and `eventHandlers` are `PartialFunction`s.** A missing case is a runtime
`MatchError`, not a compile error. Every command type and — more easily forgotten — *every* event
type needs a case, including events that do not change state. `LastOperationsUndone` above
returns `account` unchanged: the undo is applied by the framework's no-op mechanism, but the
partial function still has to accept the event.

**`initialAggregateRoot` is not persisted.** It exists so `FirstCommand` handlers have a non-null
value to pattern match against. Do not read meaningful state from it, and do not treat
`BankAccount("", 0L)` as a real account.

**`account` is unusable in a `FirstCommand` branch.** There is no aggregate yet, so it is the
initial root. `openAccount(c)` above deliberately ignores it.

**`version` is the aggregate's schema version.** Bump it when the meaning of the aggregate changes
in a way that invalidates read models built from it.

## 7. Register it

One command bus per aggregate type:

```scala
val accountsCommandBus = system.actorOf(
  AggregateCommandBusActor(new BankAccountAggregateContext, uidGenerator, eventStoreState,
    commandResponseState, eventBusActor, eventsReplayMode = false),
  "BankAccountCommandBus")
```

Optional tuning parameters follow: `maxInactivityMillis` (default 12 hours) and `keepAliveLimit`
(default 200) control how long idle per-instance child actors are kept cached. See
[operations.md](operations.md#actor-caching).

## Choosing aggregate boundaries

This is the design decision with the most consequences, and the framework's mechanics push you in
a specific direction:

- **All writes to one aggregate are serialised** through a single actor. An aggregate that
  everything touches becomes a bottleneck. "One `Company` aggregate holding all employees" is the
  classic mistake.
- **There are no transactions across aggregates.** If two things must change atomically, they
  belong in the same aggregate. If they can be eventually consistent, split them and use a saga.
- **Stream length is load latency.** No snapshots means loading an aggregate replays all its
  events. An aggregate that receives events indefinitely — a global counter, an append-only log —
  will get slow. Prefer aggregates with a natural lifecycle. The framework's
  `aggregateVersionLimit` defaults to 10 000 events per aggregate.

A good default: one aggregate per real-world entity with a lifecycle (an account, an order, a
booking), and projections for anything that spans them.

## Next

- [02-commands.md](02-commands.md) — flavors, concurrency, async, idempotency
- [03-events.md](03-events.md) — undo, duplication, deletion, schema evolution
