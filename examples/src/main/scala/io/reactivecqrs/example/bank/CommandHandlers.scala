package io.reactivecqrs.example.bank

import io.reactivecqrs.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Command handlers decide. They receive the current aggregate state and either produce events
 * ([[CommandSuccess]]) or reject the request ([[CommandFailure]]).
 *
 * Rules that matter:
 *  - A handler must not perform side effects. Returning an event *is* the effect; the framework
 *    persists it transactionally and only then applies the event handler.
 *  - A handler may return several events at once — they are persisted in one transaction and
 *    the version increments once per event.
 *  - For anything that has to wait on I/O, return an [[AsyncCommandResult]] rather than blocking.
 *    Blocking here blocks the aggregate: one `AggregateRepositoryActor` serialises all writes
 *    for a single aggregate instance.
 */
object CommandHandlers {

  /**
   * Asynchronous handler. `AggregateContext` defines an implicit conversion from
   * `Future[CustomCommandResult[T]]` to `AsyncCommandResult[T]`, so returning the future
   * directly is enough — but note the future runs on a dispatcher thread, so it must not
   * touch actor state.
   */
  def openAccount(command: OpenAccount) = Future {
    if (command.owner.trim.isEmpty) {
      CommandFailure("Owner must not be blank")
    } else if (command.initialBalance < 0) {
      CommandFailure("Initial balance must not be negative")
    } else {
      CommandSuccess(AccountOpened(command.owner.trim, command.initialBalance))
    }
  }

  def deposit(command: Deposit): CommandResult =
    if (command.amount <= 0) {
      CommandFailure("Deposit amount must be positive")
    } else {
      CommandSuccess(MoneyDeposited(command.amount))
    }

  /**
   * The interesting case: a decision that depends on current state. Because `Withdraw` is a
   * `ConcurrentCommand`, a losing race is retried against the refreshed aggregate, so this
   * check is re-evaluated against the balance that actually applies.
   */
  def withdraw(account: BankAccount, command: Withdraw): CommandResult =
    if (command.amount <= 0) {
      CommandFailure("Withdrawal amount must be positive")
    } else if (account.balance < command.amount) {
      CommandFailure(s"Insufficient funds: balance is ${account.balance}, requested ${command.amount}")
    } else {
      CommandSuccess(MoneyWithdrawn(command.amount))
    }

  def renameOwner(command: RenameOwner): CommandResult =
    if (command.owner.trim.isEmpty) {
      CommandFailure("Owner must not be blank")
    } else {
      CommandSuccess(OwnerRenamed(command.owner.trim))
    }

  def undoLastOperations(command: UndoLastOperations): CommandResult =
    if (command.stepsToUndo <= 0) {
      CommandFailure("Steps to undo must be positive")
    } else {
      CommandSuccess(LastOperationsUndone(command.stepsToUndo))
    }

  def closeAccount(account: BankAccount, command: CloseAccount): CommandResult =
    if (account.balance != 0) {
      CommandFailure(s"Cannot close an account with a non-zero balance (${account.balance})")
    } else {
      CommandSuccess(AccountClosed())
    }

  def purgeAccount(command: PurgeAccount): CommandResult =
    CommandSuccess(AccountPurged())

  /**
   * History-rewrite handler. It receives the past events selected by
   * `AnonymiseOwner.eventsTypes`, returns replacements for them, and additionally emits a new
   * event recording that the rewrite happened.
   */
  def anonymiseOwner(command: AnonymiseOwner,
                     events: Iterable[EventWithVersion[BankAccount]]): CommandResult = {
    val rewritten = events.map { ev =>
      ev.event match {
        case e: AccountOpened => EventWithVersion(ev.version, e.copy(owner = command.replacement))
        case e: OwnerRenamed  => EventWithVersion(ev.version, e.copy(owner = command.replacement))
        case _                => ev
      }
    }
    RewriteCommandSuccess(rewritten.toSeq, OwnerRenamed(command.replacement))
  }
}
