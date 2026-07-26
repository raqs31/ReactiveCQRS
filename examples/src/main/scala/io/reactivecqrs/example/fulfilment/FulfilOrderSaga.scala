package io.reactivecqrs.example.fulfilment

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import io.reactivecqrs.api.{CustomCommandResponse, FailureResponse, SagaStep, SuccessResponse}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga._
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object FulfilOrderSaga {

  /** Public entry point: fulfil a paid order via a chosen carrier. */
  case class FulfilOrder(userId: UserId, orderId: AggregateId, carrier: String) extends SagaOrder

  /** Internal steps. These are persisted to the `sagas` table, so treat their shape as a schema. */
  private case class DispatchStep(userId: UserId, orderId: AggregateId, shipmentId: AggregateId,
                                  carrier: String, trackingCode: String) extends SagaInternalOrder
  private case class MarkShippedStep(userId: UserId, orderId: AggregateId, shipmentId: AggregateId)
    extends SagaInternalOrder
  private case class CancelOrderStep(userId: UserId, orderId: AggregateId, reason: String) extends SagaInternalOrder

  sealed trait FulfilmentResponse extends SagaResponse
  case class OrderFulfilled(orderId: AggregateId, shipmentId: AggregateId, trackingCode: String) extends FulfilmentResponse
  case class FulfilmentFailed(orderId: AggregateId, reasons: List[String]) extends FulfilmentResponse
}

import FulfilOrderSaga._

/**
 * Coordinates the Order and Shipment aggregates.
 *
 * Happy path:
 * {{{
 *   FulfilOrder -> CreateShipment -> DispatchShipment -> MarkShipped -> OrderFulfilled
 * }}}
 *
 * When dispatch fails (carrier [[Carriers.Failing]]), the shipment already exists, so the saga
 * cannot simply give up — it compensates:
 * {{{
 *   SagaFailed -> REVERTING -> CancelShipment -> CancelOrder -> done
 * }}}
 *
 * Remember the revert contract: `handleRevert` is called with the order that **failed**, not the
 * one before it. So the `DispatchStep` revert case is the compensation for "the shipment exists but
 * could not be dispatched".
 */
class FulfilOrderSaga(val state: SagaState,
                      val uidGenerator: ActorRef,
                      ordersBus: ActorRef,
                      shipmentsBus: ActorRef) extends SagaActor {

  import context.dispatcher

  override val name = "FulfilOrderSaga"

  private val saga = LoggerFactory.getLogger("saga")

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case o: FulfilOrder     => createShipment(sagaStep, o)
    case o: DispatchStep    => dispatchShipment(sagaStep, o)
    case o: MarkShippedStep => markOrderShipped(sagaStep, o)
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case o: DispatchStep    => cancelShipment(sagaStep, o.userId, o.orderId, o.shipmentId, "dispatch failed")
    case o: MarkShippedStep => cancelShipment(sagaStep, o.userId, o.orderId, o.shipmentId, "could not mark order shipped")
    case o: CancelOrderStep => cancelOrder(sagaStep, o)
  }

  // --- forward path ----------------------------------------------------------------------------

  private def createShipment(sagaStep: SagaStep, order: FulfilOrder): Future[SagaHandlingStatus] = {
    saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] order=${order.orderId.asLong} creating shipment via ${order.carrier}")
    (shipmentsBus ? CreateShipment(Some(sagaStep), order.userId, order.orderId.asLong, order.carrier))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case s: SuccessResponse =>
          val tracking = "TRK-" + order.orderId.asLong + "-" + s.aggregateId.asLong
          saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] order=${order.orderId.asLong} shipment=${s.aggregateId.asLong} created")
          SagaContinues(DispatchStep(order.userId, order.orderId, s.aggregateId, order.carrier, tracking))
        case f: FailureResponse =>
          // Nothing has happened yet, so there is nothing to compensate. Report and stop.
          saga.warn(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] order=${order.orderId.asLong} shipment creation refused: ${f.exceptions.mkString(", ")}")
          SagaSucceded(FulfilmentFailed(order.orderId, f.exceptions))
      }
  }

  private def dispatchShipment(sagaStep: SagaStep, step: DispatchStep): Future[SagaHandlingStatus] =
    (shipmentsBus ? DispatchShipment(Some(sagaStep), step.userId, step.shipmentId, step.trackingCode))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case _: SuccessResponse =>
          saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] shipment=${step.shipmentId.asLong} dispatched ${step.trackingCode}")
          SagaContinues(MarkShippedStep(step.userId, step.orderId, step.shipmentId))
        case f: FailureResponse =>
          // The shipment exists. Failing here sends us into REVERTING with this same step.
          saga.warn(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] shipment=${step.shipmentId.asLong} dispatch FAILED: ${f.exceptions.mkString(", ")} -> compensating")
          SagaFailed(FulfilmentFailed(step.orderId, f.exceptions))
      }

  private def markOrderShipped(sagaStep: SagaStep, step: MarkShippedStep): Future[SagaHandlingStatus] =
    (ordersBus ? MarkShipped(Some(sagaStep), step.userId, step.orderId, step.shipmentId.asLong))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case _: SuccessResponse =>
          saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] order=${step.orderId.asLong} marked shipped - saga complete")
          SagaSucceded(OrderFulfilled(step.orderId, step.shipmentId, "TRK-" + step.orderId.asLong + "-" + step.shipmentId.asLong))
        case f: FailureResponse =>
          saga.warn(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] order=${step.orderId.asLong} could not be marked shipped: ${f.exceptions.mkString(", ")} -> compensating")
          SagaFailed(FulfilmentFailed(step.orderId, f.exceptions))
      }

  // --- compensation ----------------------------------------------------------------------------

  private def cancelShipment(sagaStep: SagaStep, userId: UserId, orderId: AggregateId,
                             shipmentId: AggregateId, reason: String): Future[SagaRevertHandlingStatus] = {
    saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] REVERT cancelling shipment=${shipmentId.asLong} ($reason)")
    (shipmentsBus ? CancelShipment(Some(sagaStep), userId, shipmentId, reason))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case _: SuccessResponse => SagaRevertContinues(CancelOrderStep(userId, orderId, reason))
        case f: FailureResponse => SagaRevertFailed(f.exceptions)
      }
  }

  private def cancelOrder(sagaStep: SagaStep, step: CancelOrderStep): Future[SagaRevertHandlingStatus] = {
    saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] REVERT cancelling order=${step.orderId.asLong} (${step.reason})")
    (ordersBus ? CancelOrder(Some(sagaStep), step.userId, step.orderId, step.reason))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case _: SuccessResponse =>
          saga.info(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] REVERT complete for order=${step.orderId.asLong}")
          SagaRevertSucceded
        case f: FailureResponse =>
          // Note the framework's behaviour here: a failed compensation is logged and the saga row is
          // dropped. There is no further retry, so compensating commands should be near-infallible.
          saga.error(s"[${sagaStep.sagaId.asLong}/${sagaStep.step}] REVERT FAILED for order=${step.orderId.asLong}: ${f.exceptions.mkString(", ")}")
          SagaRevertFailed(f.exceptions)
      }
  }
}
