package io.reactivecqrs.example.bank

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}

/**
 * Commands are requests — they may be rejected. The *flavor* you extend decides how
 * `AggregateCommandBusActor` routes the command and how concurrency is handled:
 *
 *  - [[FirstCommand]]                    creates a new aggregate; no `aggregateId` yet, the bus allocates one.
 *  - [[Command]]                         carries `expectedVersion`; a strict optimistic lock. If the aggregate
 *                                        moved on, you get `AggregateConcurrentModificationError` back and
 *                                        nothing is persisted. Use this when a human is looking at a version.
 *  - [[ConcurrentCommand]]               no expected version; runs against whatever the current version is and
 *                                        is **retried automatically** on concurrent modification. Use this for
 *                                        commutative operations and for anything a saga issues.
 *  - [[RewriteHistoryCommand]]           rewrites already-persisted events (see `eventsTypes`).
 *  - [[RewriteHistoryConcurrentCommand]] same, without an expected version.
 *
 * [[IdempotentCommand]] is orthogonal: when `idempotencyId` is set, the command's response is
 * cached in `commands_responses` and a replay of the same key returns the stored response
 * instead of executing again. `SagaStep` is the canonical key — that is what makes a saga
 * safe to resume after a crash.
 */

case class OpenAccount(idempotencyId: Option[SagaStep], userId: UserId, owner: String, initialBalance: Long)
  extends FirstCommand[BankAccount, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

/** Commutative and saga-issued, so `ConcurrentCommand`: retried rather than rejected. */
case class Deposit(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, amount: Long)
  extends ConcurrentCommand[BankAccount, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

/** Can fail on insufficient funds, but still safe to retry against a newer version. */
case class Withdraw(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, amount: Long)
  extends ConcurrentCommand[BankAccount, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

/** Strict optimistic lock: the caller asserts which version it based its decision on. */
case class RenameOwner(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, owner: String)
  extends Command[BankAccount, CustomCommandResponse[_]]

case class UndoLastOperations(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, stepsToUndo: Int)
  extends Command[BankAccount, CustomCommandResponse[_]]

case class CloseAccount(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[BankAccount, CustomCommandResponse[_]]

case class PurgeAccount(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[BankAccount, CustomCommandResponse[_]]

/**
 * Rewrites history in place. `eventsTypes` tells the framework which past events to load and
 * hand to `rewriteHistoryCommandHandlers` so they can be replaced. Use this only for things
 * like anonymisation — it destroys the original record.
 */
case class AnonymiseOwner(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, replacement: String)
  extends RewriteHistoryCommand[BankAccount, CustomCommandResponse[_]] {
  override def eventsTypes: Set[Class[_]] = Set(classOf[AccountOpened], classOf[OwnerRenamed])
}
