package io.reactivecqrs.example.fulfilment

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga.{SagaFailureResponse, SagaResponse}
import io.reactivecqrs.example.fulfilment.FulfilOrderSaga.{FulfilOrder, FulfilmentFailed, OrderFulfilled}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Generates randomised but **reproducible** order traffic.
 *
 * Every flow derives its own `Random` from `seed` and the order index, so a given `--seed` always
 * produces the same sequence of commands regardless of how they interleave across threads. That
 * matters for a demo you want to re-run while reading the logs.
 *
 * Each flow blocks on `ask`. That is fine here because flows run on a dedicated thread pool, never
 * on an actor thread — see the warning in `docs/guides/02-commands.md`.
 */
class TrafficGenerator(ordersBus: ActorRef,
                       shipmentsBus: ActorRef,
                       fulfilSaga: ActorRef,
                       askTimeout: FiniteDuration) {

  private implicit val timeout: Timeout = Timeout(askTimeout)

  private val cmd = LoggerFactory.getLogger("command")
  private val flow = LoggerFactory.getLogger("flow")

  private val customers = Vector("acme-ltd", "globex", "initech", "umbrella", "soylent", "hooli", "stark-ind")
  private val skus = Vector("WIDGET-1", "WIDGET-2", "GIZMO-9", "COG-77", "SPRING-4", "BOLT-12", "PLATE-3")

  private def send[T: ClassTag](target: ActorRef, message: Any): T = {
    cmd.info(s"-> ${describe(message)}")
    val response = Await.result((target ? message).mapTo[T], askTimeout)
    cmd.info(s"<- ${describe(response)}")
    response
  }

  private def describe(a: Any): String = a match {
    case SuccessResponse(id, v)                                => s"SuccessResponse(aggregate=${id.asLong}, version=${v.asInt})"
    case CustomSuccessResponse(id, v, info)                    => s"CustomSuccessResponse(aggregate=${id.asLong}, version=${v.asInt}, $info)"
    case FailureResponse(reasons)                              => s"FailureResponse(${reasons.mkString("; ")})"
    case AggregateConcurrentModificationError(id, _, exp, was) => s"ConcurrentModification(aggregate=${id.asLong}, expected=${exp.asInt}, was=${was.asInt})"
    case other                                                 => other.toString
  }

  /**
   * Runs one complete order lifecycle. Returns a short outcome label for the run summary.
   *
   * The shape is deliberately varied so a run exercises the interesting paths: successful
   * fulfilment, saga compensation when the carrier refuses, a domain rejection when the order is
   * too large, an outright cancellation, and an optimistic-lock conflict.
   */
  def runOrder(index: Int, seed: Long, userId: UserId): String = {
    val rnd = new Random(seed * 100003L + index)
    val customer = customers(rnd.nextInt(customers.size))

    flow.info(s"=== order flow #$index for $customer ===")

    val placed = send[CustomCommandResponse[_]](ordersBus, PlaceOrder(None, userId, customer))
    placed match {
      case s: SuccessResponse => runPlacedOrder(index, rnd, userId, s.aggregateId, s.aggregateVersion)
      case other =>
        flow.warn(s"order flow #$index could not place an order: ${describe(other)}")
        "place-rejected"
    }
  }

  private def runPlacedOrder(index: Int, rnd: Random, userId: UserId,
                             orderId: AggregateId, placedVersion: AggregateVersion): String = {

    // `Command` carries an expected version, so we track the version the aggregate is at.
    var version = placedVersion

    val lineCount = 1 + rnd.nextInt(4)
    (1 to lineCount).foreach { _ =>
      val sku = skus(rnd.nextInt(skus.size))
      val qty = 1 + rnd.nextInt(5)
      // Occasionally expensive, so the payment limit in the command handler is actually reachable.
      val price = if (rnd.nextInt(10) == 0) 90000L + rnd.nextInt(60000) else 500L + rnd.nextInt(9000)
      send[CustomCommandResponse[_]](ordersBus, AddLine(userId, orderId, version, sku, qty, price)) match {
        case s: SuccessResponse => version = s.aggregateVersion
        case _                  => () // rejected: version did not move
      }
    }

    // Deliberately send a stale expected version now and then, to show the strict optimistic lock
    // rejecting rather than silently retrying.
    if (rnd.nextInt(6) == 0) {
      flow.info(s"order flow #$index sending a deliberately stale expected version")
      send[CustomCommandResponse[_]](ordersBus, AddLine(userId, orderId, AggregateVersion(1), "STALE-1", 1, 100L))
    }

    if (rnd.nextInt(10) < 2) {
      send[CustomCommandResponse[_]](ordersBus, CancelOrder(None, userId, orderId, "customer changed their mind"))
      flow.info(s"order flow #$index cancelled by customer")
      return "cancelled-by-customer"
    }

    send[CustomCommandResponse[_]](ordersBus, PayOrder(userId, orderId, version)) match {
      case s: SuccessResponse =>
        version = s.aggregateVersion
        fulfil(index, rnd, userId, orderId)
      case f: FailureResponse =>
        flow.info(s"order flow #$index payment rejected: ${f.exceptions.mkString("; ")}")
        send[CustomCommandResponse[_]](ordersBus, CancelOrder(None, userId, orderId, "payment rejected"))
        "payment-rejected"
      case other =>
        flow.warn(s"order flow #$index unexpected payment response: ${describe(other)}")
        "payment-error"
    }
  }

  private def fulfil(index: Int, rnd: Random, userId: UserId, orderId: AggregateId): String = {
    val carrier = Carriers.All(rnd.nextInt(Carriers.All.size))
    flow.info(s"order flow #$index handing order=${orderId.asLong} to the fulfilment saga via $carrier")

    val response = Await.result((fulfilSaga ? FulfilOrder(userId, orderId, carrier)).mapTo[SagaResponse], askTimeout)
    response match {
      case OrderFulfilled(id, shipmentId, tracking) =>
        flow.info(s"order flow #$index FULFILLED order=${id.asLong} shipment=${shipmentId.asLong} $tracking")
        maybeDeliver(index, rnd, userId, id, shipmentId)
      case FulfilmentFailed(id, reasons) =>
        flow.info(s"order flow #$index fulfilment failed for order=${id.asLong}: ${reasons.mkString("; ")}")
        "fulfilment-compensated"
      case SagaFailureResponse(reasons) =>
        flow.warn(s"order flow #$index saga error: ${reasons.mkString("; ")}")
        "saga-error"
      case other =>
        flow.warn(s"order flow #$index unexpected saga response: $other")
        "saga-unexpected"
    }
  }

  /** Most parcels arrive. This closes the lifecycle so the read models show terminal states. */
  private def maybeDeliver(index: Int, rnd: Random, userId: UserId,
                           orderId: AggregateId, shipmentId: AggregateId): String =
    if (rnd.nextInt(10) < 7) {
      send[CustomCommandResponse[_]](shipmentsBus, DeliverShipment(None, userId, shipmentId))
      send[CustomCommandResponse[_]](ordersBus, MarkDelivered(None, userId, orderId))
      flow.info(s"order flow #$index delivered")
      "delivered"
    } else {
      flow.info(s"order flow #$index left in transit")
      "in-transit"
    }
}
