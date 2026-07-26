package io.reactivecqrs.example.bank

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.eventbus.EventBusSubscriptionsManagerApi
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import scalikejdbc.DBSession

/** The read model. Stored as JSONB in a `projection_<name>` table. */
case class AccountSummary(owner: String, balance: Long)

/** A read model that only counts, to show an event-driven projection. */
case class AccountActivity(deposits: Int, withdrawals: Int, totalMoved: Long)

object AccountsProjection {
  case object GetAllAccounts
  case class GetAccount(id: AggregateId)
  case object GetAllActivity
}

import AccountsProjection._

/**
 * Projections are **eventually consistent**. A command returns as soon as its events are
 * persisted; the event bus delivers them to projections afterwards. Do not read a projection
 * immediately after a write and expect to see the change — if you need read-your-writes, query
 * the aggregate through the command bus instead.
 *
 * Projections must be **idempotent**. The event bus can redeliver, and a projection rebuild
 * replays everything from the beginning.
 *
 * ==Aggregate-based projection==
 *
 * An `AggregateListener` receives the whole aggregate after each change: `(id, version,
 * created, Some(root))`, or `None` once the aggregate is deleted. This is the simplest style —
 * you mirror current state and never have to reason about which event did what. Use it when the
 * read model is a function of current state.
 */
class AccountsSummaryProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                val subscriptionsState: SubscriptionsState,
                                documentStore: DocumentStore[AccountSummary]) extends ProjectionActor {

  override protected val projectionName: String = "AccountsSummaryProjection"

  /**
   * Bumping this version tells the framework the read model's shape changed and the projection
   * needs rebuilding from the event store.
   */
  override protected val version: Int = 1

  protected val listeners = List(AggregateListener(accountChanged))

  private def accountChanged(id: AggregateId,
                             aggregateVersion: AggregateVersion,
                             created: Boolean,
                             account: Option[BankAccount]) = { implicit session: DBSession =>
    account match {
      case Some(a) if created => documentStore.insertDocument(0L, id.asLong, AccountSummary(a.owner, a.balance))
      case Some(a)            => documentStore.overwriteDocument(id.asLong, AccountSummary(a.owner, a.balance))
      case None               => documentStore.removeDocument(id.asLong)
    }
  }

  /**
   * Queries are served by the projection actor itself. Everything here runs on the actor thread,
   * so keep it cheap — push filtering into the document store rather than loading and scanning.
   */
  override protected def receiveQuery: Receive = {
    case GetAllAccounts =>
      sender() ! documentStore.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
    case GetAccount(id) =>
      sender() ! documentStore.getDocument(id.asLong).map(_.document)
  }

  /** Called before a rebuild. Wipe whatever this projection owns. */
  override protected def onClearProjectionData(): Unit = {
    // A real projection truncates its table here. The example's store is recreated per run.
  }
}

/**
 * ==Event-based projection==
 *
 * An `EventsListener` receives the events themselves, in order, batched per aggregate. Use it
 * when the read model is a function of *what happened* rather than of current state — counters,
 * audit logs, time series. The aggregate root is not available here.
 *
 * Note the callback receives a `Seq[EventInfo[_]]`: several events can arrive in one batch, and
 * each carries its own version, `userId` and timestamp.
 */
class AccountsActivityProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                 val subscriptionsState: SubscriptionsState,
                                 documentStore: DocumentStore[AccountActivity]) extends ProjectionActor {

  override protected val projectionName: String = "AccountsActivityProjection"
  override protected val version: Int = 1

  protected val listeners = List(EventsListener(accountEvents))

  private def accountEvents(id: AggregateId, events: Seq[EventInfo[BankAccount]]) = { implicit session: DBSession =>
    events.foreach(info => applyEvent(id, info))
  }

  private def applyEvent(id: AggregateId, info: EventInfo[BankAccount])(implicit session: DBSession): Unit =
    info.event match {
      case _: AccountOpened =>
        documentStore.insertDocument(0L, id.asLong, AccountActivity(0, 0, 0L))
      case MoneyDeposited(amount) =>
        bump(id)(a => a.copy(deposits = a.deposits + 1, totalMoved = a.totalMoved + amount))
      case MoneyWithdrawn(amount) =>
        bump(id)(a => a.copy(withdrawals = a.withdrawals + 1, totalMoved = a.totalMoved + amount))
      case _: AccountClosed | _: AccountPurged =>
        documentStore.removeDocument(id.asLong)
      case _ =>
        () // events this read model does not care about
    }

  private def bump(id: AggregateId)(f: AccountActivity => AccountActivity)(implicit session: DBSession): Unit =
    documentStore.updateDocument(0L, id.asLong, {
      case Some(doc) => Document(f(doc.document))
      case None      => Document(f(AccountActivity(0, 0, 0L)))
    })

  override protected def receiveQuery: Receive = {
    case GetAllActivity =>
      sender() ! documentStore.findAll().map { case (id, doc) => AggregateId(id) -> doc.document }
  }

  override protected def onClearProjectionData(): Unit = ()
}
