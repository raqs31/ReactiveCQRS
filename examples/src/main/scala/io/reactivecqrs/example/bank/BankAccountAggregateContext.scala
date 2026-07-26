package io.reactivecqrs.example.bank

import io.reactivecqrs.api._

import CommandHandlers._
import EventHandlers._

/**
 * The single object a domain author implements. It binds commands to decisions, events to state
 * transitions, and declares the event schema version map.
 *
 * One instance is created per aggregate *type* and handed to `AggregateCommandBusActor`.
 */
class BankAccountAggregateContext extends AggregateContext[BankAccount] {

  /**
   * Bump this when the meaning of the aggregate changes in a way that invalidates read models
   * built from it. Projections compare their own version against stored subscriptions to decide
   * whether they need a rebuild.
   */
  override val version: Int = 1

  /**
   * Event schema versioning. `EV[Base](version -> class, ...)` maps a stored
   * `event_type_id`/`event_type_version` pair back to a concrete class, which lets you rename or
   * reshape an event without breaking replay of the events already on disk.
   *
   * Concretely: if `AccountOpened` ever has to change shape, keep the old class (say
   * `AccountOpenedV0`), add the new one, and list both here — version 0 deserialises into the
   * old class, version 1 into the new.
   */
  override val eventsVersions: List[EventVersion[BankAccount]] =
    EV[AccountOpened](0 -> classOf[AccountOpened]) ::
    EV[OwnerRenamed](0 -> classOf[OwnerRenamed]) ::
    Nil

  /**
   * `account` is the current aggregate state. For a `FirstCommand` it is `initialAggregateRoot`,
   * since nothing exists yet — do not read it in that branch.
   *
   * This is a `PartialFunction`: a command with no matching case is an error at runtime, not at
   * compile time. Keep it exhaustive.
   */
  override def commandHandlers = account => {
    case c: OpenAccount        => openAccount(c)
    case c: Deposit            => deposit(c)
    case c: Withdraw           => withdraw(account, c)
    case c: RenameOwner        => renameOwner(c)
    case c: UndoLastOperations => undoLastOperations(c)
    case c: CloseAccount       => closeAccount(account, c)
    case c: PurgeAccount       => purgeAccount(c)
  }

  override def rewriteHistoryCommandHandlers = (events, account) => {
    case c: AnonymiseOwner => anonymiseOwner(c, events)
  }

  /**
   * Applies an event to state. `userId` and `timestamp` come from the persisted event row, not
   * from the current clock — that is what makes replay deterministic.
   *
   * Returning `null` marks the aggregate deleted (the framework stores it as `None`).
   *
   * Every event type must appear here, including the ones that do not change state
   * (`LastOperationsUndone` is handled by the framework's no-op mechanism, but the partial
   * function still has to accept it).
   */
  override def eventHandlers = (userId, timestamp, account) => {
    case e: AccountOpened        => accountOpened(e)
    case e: MoneyDeposited       => moneyDeposited(account, e)
    case e: MoneyWithdrawn       => moneyWithdrawn(account, e)
    case e: OwnerRenamed         => ownerRenamed(account, e)
    case e: LastOperationsUndone => account
    case e: AccountClosed        => null
    case e: AccountPurged        => null
  }

  /**
   * The state a `FirstCommand` handler sees before any event exists. It is never persisted —
   * it only exists so command handlers have a non-null value to pattern match against.
   */
  override def initialAggregateRoot: BankAccount = BankAccount("", 0L)
}
