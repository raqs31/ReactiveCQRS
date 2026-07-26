package io.reactivecqrs.example.fulfilment

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.AggregateId
import io.reactivecqrs.core.documentstore.{Document, DocumentStore}
import io.reactivecqrs.core.eventbus.EventBusSubscriptionsManagerApi
import io.reactivecqrs.core.projection.{ProjectionActor, SubscriptionsState}
import org.slf4j.LoggerFactory
import scalikejdbc.DBSession

// ---------------------------------------------------------------------------------------------
// Read models. Kept to primitives — these are serialized to JSONB.
// ---------------------------------------------------------------------------------------------

case class OrderSummary(customer: String, status: String, lineCount: Int, totalCents: Long, shipmentId: Long)

case class ShipmentSummary(orderId: Long, carrier: String, status: String, trackingCode: String)

case class FulfilmentStats(ordersPlaced: Int, ordersPaid: Int, ordersShipped: Int, ordersCancelled: Int,
                           shipmentsCreated: Int, shipmentsDispatched: Int, shipmentsCancelled: Int)

object FulfilmentProjections {
  case object GetOrders
  case object GetShipments
  case object GetStats
  case class GetOrder(id: AggregateId)

  val StatsKey = 0L
}

import FulfilmentProjections._

/**
 * Current state of every order. An `AggregateListener` receives the whole aggregate root after each
 * change, which makes this projection trivially idempotent — it always writes the current value
 * rather than applying a delta.
 */
class OrdersProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                       val subscriptionsState: SubscriptionsState,
                       documentStore: DocumentStore[OrderSummary]) extends ProjectionActor {

  override protected val projectionName: String = "FulfilmentOrdersProjection"
  override protected val version: Int = 1

  private val proj = LoggerFactory.getLogger("projection.orders")

  protected val listeners = List(AggregateListener(orderChanged))

  private def orderChanged(id: AggregateId, v: AggregateVersion, created: Boolean,
                           order: Option[Order]) = { implicit session: DBSession =>
    order match {
      case Some(o) =>
        val summary = OrderSummary(o.customer, o.status, o.lines.size, o.totalCents, o.shipmentId)
        if (created) documentStore.insertDocument(0L, id.asLong, summary)
        else         documentStore.overwriteDocument(id.asLong, summary)
        proj.info(f"order=${id.asLong}%-4d v${v.asInt}%-3d ${o.status}%-9s lines=${o.lines.size} total=${o.totalCents}%6d ${o.customer}")
      case None =>
        documentStore.removeDocument(id.asLong)
        proj.info(s"order=${id.asLong} removed")
    }
  }

  override protected def receiveQuery: Receive = {
    case GetOrders   => sender() ! documentStore.findAll().map { case (k, d) => k -> d.document }
    case GetOrder(i) => sender() ! documentStore.getDocument(i.asLong).map(_.document)
  }

  override protected def onClearProjectionData(): Unit = ()
}

/** Current state of every shipment. */
class ShipmentsProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                          val subscriptionsState: SubscriptionsState,
                          documentStore: DocumentStore[ShipmentSummary]) extends ProjectionActor {

  override protected val projectionName: String = "FulfilmentShipmentsProjection"
  override protected val version: Int = 1

  private val proj = LoggerFactory.getLogger("projection.shipments")

  protected val listeners = List(AggregateListener(shipmentChanged))

  private def shipmentChanged(id: AggregateId, v: AggregateVersion, created: Boolean,
                              shipment: Option[Shipment]) = { implicit session: DBSession =>
    shipment match {
      case Some(s) =>
        val summary = ShipmentSummary(s.orderId, s.carrier, s.status, s.trackingCode)
        if (created) documentStore.insertDocument(0L, id.asLong, summary)
        else         documentStore.overwriteDocument(id.asLong, summary)
        proj.info(f"shipment=${id.asLong}%-4d v${v.asInt}%-3d ${s.status}%-10s ${s.carrier}%-9s order=${s.orderId} ${s.trackingCode}")
      case None =>
        documentStore.removeDocument(id.asLong)
        proj.info(s"shipment=${id.asLong} removed")
    }
  }

  override protected def receiveQuery: Receive = {
    case GetShipments => sender() ! documentStore.findAll().map { case (k, d) => k -> d.document }
  }

  override protected def onClearProjectionData(): Unit = ()
}

/**
 * A read model spanning **both** aggregate types, and built from events rather than state.
 *
 * Two things are worth noticing here:
 *
 *  - a single projection can register listeners for several aggregate types, which is how you build
 *    a read model that no single aggregate could produce;
 *  - counters are the classic idempotency hazard. The event bus delivers at-least-once, so a
 *    redelivery would double-count. A production version would record the last processed version per
 *    aggregate in the document and skip anything at or below it. This one does not — it is a demo
 *    dashboard, and the simplification is called out rather than hidden.
 */
class FulfilmentStatsProjection(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                val subscriptionsState: SubscriptionsState,
                                documentStore: DocumentStore[FulfilmentStats]) extends ProjectionActor {

  override protected val projectionName: String = "FulfilmentStatsProjection"
  override protected val version: Int = 1

  private val proj = LoggerFactory.getLogger("projection.stats")

  protected val listeners: List[Listener[Any]] =
    List(EventsListener(orderEvents), EventsListener(shipmentEvents))

  private def orderEvents(id: AggregateId, events: Seq[EventInfo[Order]]) = { implicit session: DBSession =>
    events.foreach { info =>
      proj.debug(s"order=${id.asLong} v${info.version.asInt} ${info.event.getClass.getSimpleName}")
      info.event match {
        case _: OrderPlaced    => bump(s => s.copy(ordersPlaced = s.ordersPlaced + 1))
        case _: OrderPaid      => bump(s => s.copy(ordersPaid = s.ordersPaid + 1))
        case _: OrderShipped   => bump(s => s.copy(ordersShipped = s.ordersShipped + 1))
        case _: OrderCancelled => bump(s => s.copy(ordersCancelled = s.ordersCancelled + 1))
        case _                 => ()
      }
    }
  }

  private def shipmentEvents(id: AggregateId, events: Seq[EventInfo[Shipment]]) = { implicit session: DBSession =>
    events.foreach { info =>
      proj.debug(s"shipment=${id.asLong} v${info.version.asInt} ${info.event.getClass.getSimpleName}")
      info.event match {
        case _: ShipmentCreated    => bump(s => s.copy(shipmentsCreated = s.shipmentsCreated + 1))
        case _: ShipmentDispatched => bump(s => s.copy(shipmentsDispatched = s.shipmentsDispatched + 1))
        case _: ShipmentCancelled  => bump(s => s.copy(shipmentsCancelled = s.shipmentsCancelled + 1))
        case _                     => ()
      }
    }
  }

  private def bump(f: FulfilmentStats => FulfilmentStats)(implicit session: DBSession): Unit =
    documentStore.updateDocument(0L, StatsKey, {
      case Some(doc) => Document(f(doc.document))
      case None      => Document(f(FulfilmentStats(0, 0, 0, 0, 0, 0, 0)))
    })

  override protected def receiveQuery: Receive = {
    case GetStats => sender() ! documentStore.getDocument(StatsKey).map(_.document)
  }

  override protected def onClearProjectionData(): Unit = ()
}
