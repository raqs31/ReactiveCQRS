package io.reactivecqrs.example.fulfilment

import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, SpaceId, UserId}

/**
 * The Order aggregate.
 *
 * Unlike the `bank` example — which spreads one concept per file for teaching — this example keeps
 * a whole aggregate in one file, which is closer to how you would actually lay out a domain.
 *
 * Status is a plain `String` rather than a sealed trait on purpose: aggregate roots travel through
 * the event bus and read models are serialized to JSON, and sealed-trait ADTs are the shape most
 * likely to surprise you there. Nested case classes (`OrderLine`) are fine.
 */
case class Order(customer: String,
                 lines: Vector[OrderLine],
                 status: String,
                 shipmentId: Long,
                 cancelReason: String) {

  def totalCents: Long = lines.map(l => l.quantity.toLong * l.unitPriceCents).sum
}

case class OrderLine(sku: String, quantity: Int, unitPriceCents: Long)

object OrderStatus {
  val Placed    = "PLACED"
  val Paid      = "PAID"
  val Shipped   = "SHIPPED"
  val Delivered = "DELIVERED"
  val Cancelled = "CANCELLED"
}

// ---------------------------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------------------------

case class OrderPlaced(customer: String) extends FirstEvent[Order] {
  override def spaceId: SpaceId = SpaceId(0)
}

case class LineAdded(sku: String, quantity: Int, unitPriceCents: Long) extends Event[Order]

case class LineRemoved(sku: String) extends Event[Order]

case class OrderPaid(amountCents: Long) extends Event[Order]

case class OrderShipped(shipmentId: Long) extends Event[Order]

case class OrderDelivered() extends Event[Order]

case class OrderCancelled(reason: String) extends Event[Order]

// ---------------------------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------------------------

case class PlaceOrder(idempotencyId: Option[SagaStep], userId: UserId, customer: String)
  extends FirstCommand[Order, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

/** User-driven, so strict: the caller saw a specific version of the basket. */
case class AddLine(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   sku: String, quantity: Int, unitPriceCents: Long)
  extends Command[Order, CustomCommandResponse[_]]

case class RemoveLine(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion, sku: String)
  extends Command[Order, CustomCommandResponse[_]]

case class PayOrder(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion)
  extends Command[Order, CustomCommandResponse[_]]

/**
 * The next three are issued by the fulfilment saga, so they are `ConcurrentCommand` (a saga has no
 * sensible expected version) and `IdempotentCommand` (a resumed saga step must not apply twice).
 */
case class MarkShipped(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, shipmentId: Long)
  extends ConcurrentCommand[Order, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

case class MarkDelivered(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId)
  extends ConcurrentCommand[Order, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

case class CancelOrder(idempotencyId: Option[SagaStep], userId: UserId, aggregateId: AggregateId, reason: String)
  extends ConcurrentCommand[Order, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

// ---------------------------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------------------------

class OrderAggregateContext extends AggregateContext[Order] {

  override val version: Int = 1

  override def commandHandlers = order => {

    case c: PlaceOrder =>
      if (c.customer.trim.isEmpty) CommandFailure("Customer must not be blank")
      else                         CommandSuccess(OrderPlaced(c.customer.trim))

    case c: AddLine =>
      if (order.status != OrderStatus.Placed) CommandFailure(s"Cannot add lines to a ${order.status} order")
      else if (c.quantity <= 0)               CommandFailure("Quantity must be positive")
      else if (order.lines.size >= 8)         CommandFailure("Too many lines on this order")
      else CommandSuccess(LineAdded(c.sku, c.quantity, c.unitPriceCents))

    case c: RemoveLine =>
      if (order.status != OrderStatus.Placed)      CommandFailure(s"Cannot change a ${order.status} order")
      else if (!order.lines.exists(_.sku == c.sku)) CommandFailure(s"No such line: ${c.sku}")
      else CommandSuccess(LineRemoved(c.sku))

    case c: PayOrder =>
      if (order.status != OrderStatus.Placed) CommandFailure(s"Cannot pay a ${order.status} order")
      else if (order.lines.isEmpty)           CommandFailure("Cannot pay for an empty order")
      // A deliberately reachable rejection, so the demo exercises the failure path.
      else if (order.totalCents > 500000L)    CommandFailure(s"Order total ${order.totalCents} exceeds the payment limit")
      else CommandSuccess(OrderPaid(order.totalCents))

    case c: MarkShipped =>
      if (order.status == OrderStatus.Shipped)     CommandSuccess(Seq.empty[Event[Order]]) // already there; stay idempotent
      else if (order.status != OrderStatus.Paid)   CommandFailure(s"Cannot ship a ${order.status} order")
      else CommandSuccess(OrderShipped(c.shipmentId))

    case c: MarkDelivered =>
      if (order.status == OrderStatus.Delivered)   CommandSuccess(Seq.empty[Event[Order]])
      else if (order.status != OrderStatus.Shipped) CommandFailure(s"Cannot deliver a ${order.status} order")
      else CommandSuccess(OrderDelivered())

    case c: CancelOrder =>
      if (order.status == OrderStatus.Cancelled)   CommandSuccess(Seq.empty[Event[Order]])
      else if (order.status == OrderStatus.Delivered) CommandFailure("Cannot cancel a delivered order")
      else CommandSuccess(OrderCancelled(c.reason))
  }

  override def eventHandlers = (userId, timestamp, order) => {
    case e: OrderPlaced    => Order(e.customer, Vector.empty, OrderStatus.Placed, 0L, "")
    case e: LineAdded      => order.copy(lines = order.lines :+ OrderLine(e.sku, e.quantity, e.unitPriceCents))
    case e: LineRemoved    => order.copy(lines = order.lines.filterNot(_.sku == e.sku))
    case e: OrderPaid      => order.copy(status = OrderStatus.Paid)
    case e: OrderShipped   => order.copy(status = OrderStatus.Shipped, shipmentId = e.shipmentId)
    case e: OrderDelivered => order.copy(status = OrderStatus.Delivered)
    case e: OrderCancelled => order.copy(status = OrderStatus.Cancelled, cancelReason = e.reason)
  }

  override def initialAggregateRoot: Order = Order("", Vector.empty, OrderStatus.Placed, 0L, "")
}
