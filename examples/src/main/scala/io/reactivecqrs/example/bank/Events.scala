package io.reactivecqrs.example.bank

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.SpaceId

/**
 * Events are the durable record. They are serialized to JSON (mpjsons) and stored forever,
 * so treat them as a published schema: renaming a field or a class is a breaking change
 * unless you register a mapping in `BankAccountAggregateContext.eventsVersions`.
 *
 * Events are named in the past tense — they describe what happened, not what was asked for.
 */

/**
 * A `FirstEvent` creates the aggregate. It must supply a `SpaceId`, which is a coarse
 * partitioning key (tenant, workspace, customer). Use `SpaceId(0)` when you have no
 * partitioning scheme yet — it is stored on the `aggregates` row and cannot be changed later.
 */
case class AccountOpened(owner: String, initialBalance: Long) extends FirstEvent[BankAccount] {
  override def spaceId: SpaceId = SpaceId(0)
}

case class MoneyDeposited(amount: Long) extends Event[BankAccount]

case class MoneyWithdrawn(amount: Long) extends Event[BankAccount]

case class OwnerRenamed(owner: String) extends Event[BankAccount]

/**
 * An `UndoEvent` logically cancels the previous `eventsCount` events. The framework marks
 * them as no-ops in the `noop_events` table and rebuilds the aggregate without them — your
 * event handler does not have to reverse anything by hand, it just has to accept the event.
 */
case class LastOperationsUndone(eventsCount: Int) extends UndoEvent[BankAccount]

/**
 * A normal event whose handler returns `null`, which marks the aggregate as deleted.
 * The events remain in the store, so history is still queryable.
 */
case class AccountClosed() extends Event[BankAccount]

/**
 * A `PermanentDeleteEvent` hard-deletes the aggregate *and* its events. This is irreversible
 * and defeats event sourcing's audit trail — reserve it for erasure requests (e.g. GDPR).
 */
case class AccountPurged() extends PermanentDeleteEvent[BankAccount]
