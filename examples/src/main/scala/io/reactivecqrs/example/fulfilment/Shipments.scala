package io.reactivecqrs.example.fulfilment

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

/**
 * The Shipment aggregate — the second consistency boundary in this example.
 *
 * Orders and shipments are separate aggregates because they change for different reasons and are
 * owned by different parts of the business. That means there is no transaction spanning them, which
 * is exactly why [[FulfilOrderSaga]] exists.
 */
case class Shipment(orderId: Long,
                    carrier: String,
                    status: String,
                    trackingCode: String,
                    failureReason: String)

object ShipmentStatus {
  val Created    = "CREATED"
  val Dispatched = "DISPATCHED"
  val Delivered  = "DELIVERED"
  val Cancelled  = "CANCELLED"
}

object Carriers {
  /**
   * Dispatch always fails for this carrier. It is not randomness — a command handler that consulted
   * a random number generator would make replay non-reproducible on the retry path. Making failure
   * a deterministic function of aggregate state keeps the demo reproducible under a fixed seed while
   * still exercising saga compensation.
   */
  val Failing = "STORMLINE"

  val All: Vector[String] = Vector("DHL", "UPS", "FEDEX", "POSTNL", Failing)
}

// ---------------------------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------------------------

case class ShipmentCreated(orderId: Long, carrier: String) extends FirstEvent[Shipment] {
  override def spaceId: SpaceId = SpaceId(0)
}

case class ShipmentDispatched(trackingCode: String) extends Event[Shipment]

case class ShipmentDelivered() extends Event[Shipment]

case class ShipmentCancelled(reason: String) extends Event[Shipment]

// ---------------------------------------------------------------------------------------------
// Commands — every one is saga-issued, hence Concurrent + Idempotent throughout
// ---------------------------------------------------------------------------------------------

case class CreateShipment(idempotencyId: Option[SagaStep], userId: UserId, orderId: Long, carrier: String)
  extends FirstCommand[Shipment, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

case class DispatchShipment(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId,
                            trackingCode: String)
  extends ConcurrentCommand[Shipment, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

case class DeliverShipment(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId)
  extends ConcurrentCommand[Shipment, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

case class CancelShipment(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, reason: String)
  extends ConcurrentCommand[Shipment, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

// ---------------------------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------------------------

class ShipmentAggregateContext extends AggregateContext[Shipment] {

  override val version: Int = 1

  override def commandHandlers = shipment => {

    case c: CreateShipment =>
      if (c.orderId <= 0) CommandFailure("Shipment must reference an order")
      else                CommandSuccess(ShipmentCreated(c.orderId, c.carrier))

    case c: DispatchShipment =>
      if (shipment.status == ShipmentStatus.Dispatched)   CommandSuccess(Seq.empty[Event[Shipment]])
      else if (shipment.status != ShipmentStatus.Created) CommandFailure(s"Cannot dispatch a ${shipment.status} shipment")
      else if (shipment.carrier == Carriers.Failing)
        CommandFailure(s"Carrier ${shipment.carrier} refused the shipment (simulated outage)")
      else CommandSuccess(ShipmentDispatched(c.trackingCode))

    case c: DeliverShipment =>
      if (shipment.status == ShipmentStatus.Delivered)       CommandSuccess(Seq.empty[Event[Shipment]])
      else if (shipment.status != ShipmentStatus.Dispatched) CommandFailure(s"Cannot deliver a ${shipment.status} shipment")
      else CommandSuccess(ShipmentDelivered())

    case c: CancelShipment =>
      if (shipment.status == ShipmentStatus.Cancelled)      CommandSuccess(Seq.empty[Event[Shipment]])
      else if (shipment.status == ShipmentStatus.Delivered) CommandFailure("Cannot cancel a delivered shipment")
      else CommandSuccess(ShipmentCancelled(c.reason))
  }

  override def eventHandlers = (userId, timestamp, shipment) => {
    case e: ShipmentCreated    => Shipment(e.orderId, e.carrier, ShipmentStatus.Created, "", "")
    case e: ShipmentDispatched => shipment.copy(status = ShipmentStatus.Dispatched, trackingCode = e.trackingCode)
    case e: ShipmentDelivered  => shipment.copy(status = ShipmentStatus.Delivered)
    case e: ShipmentCancelled  => shipment.copy(status = ShipmentStatus.Cancelled, failureReason = e.reason)
  }

  override def initialAggregateRoot: Shipment = Shipment(0L, "", ShipmentStatus.Created, "", "")
}
