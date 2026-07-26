package io.reactivecqrs.example.bank

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.pattern.ask
import io.reactivecqrs.api.{CustomCommandResponse, FailureResponse, SagaStep, SuccessResponse}
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.saga._

import scala.concurrent.Future

object MoneyTransferSaga {

  /** The public entry point. Send this to the saga actor to start a transfer. */
  case class TransferMoney(userId: UserId, from: AggregateId, to: AggregateId, amount: Long) extends SagaOrder

  /** Internal steps are `SagaInternalOrder` — they are persisted and resumed, but not part of the API. */
  private case class CreditTarget(userId: UserId, from: AggregateId, to: AggregateId, amount: Long) extends SagaInternalOrder

  sealed trait MoneyTransferResponse extends SagaResponse
  case class MoneyTransferred(from: AggregateId, to: AggregateId, amount: Long) extends MoneyTransferResponse
  case class MoneyTransferFailed(reasons: List[String]) extends MoneyTransferResponse
}

import MoneyTransferSaga._

/**
 * A saga (process manager) coordinates a change that spans several aggregates. There are no
 * distributed transactions here: each step is its own local transaction, and failure is handled
 * by *compensating* — undoing the earlier steps with new commands, not by rolling back.
 *
 * ==How the framework drives it==
 *
 *  1. A `SagaOrder` arrives. Progress is written to the `sagas` table *before* the step runs.
 *  2. `handleOrder` returns a `Future[SagaHandlingStatus]`:
 *     - [[SagaContinues]] — persist the next internal order and keep going (phase `CONTINUES`).
 *     - [[SagaSucceded]]  — reply to the original sender and delete the saga row.
 *     - [[SagaFailed]]    — reply, then re-enter the **same order** in phase `REVERTING`.
 *  3. In `REVERTING`, `handleRevert` is called with the order that failed, and walks backwards
 *     via [[SagaRevertContinues]] until [[SagaRevertSucceded]].
 *
 * Because progress is persisted at every step, a crash mid-transfer is resumed on restart
 * (`loadAllSagas` in `preStart`). That is exactly why every command a saga issues carries
 * `Some(sagaStep)` as its idempotency id: on resume the step may be re-issued, and the cached
 * response in `commands_responses` prevents the money moving twice.
 *
 * Note the failure semantics of `handleRevert`: it matches on the order *that failed*. Here,
 * `CreditTarget` failing means the source was already debited, so the compensation is to
 * refund it.
 */
class MoneyTransferSaga(val state: SagaState,
                        val uidGenerator: ActorRef,
                        accountsCommandBus: ActorRef) extends SagaActor {

  import context.dispatcher

  /** Identifies this saga's rows in the `sagas` table. Changing it orphans in-flight sagas. */
  override val name = "MoneyTransferSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case order: TransferMoney => debitSource(sagaStep, order)
    case order: CreditTarget  => creditTarget(sagaStep, order)
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case order: CreditTarget => refundSource(sagaStep, order)
  }

  private def debitSource(sagaStep: SagaStep, order: TransferMoney): Future[SagaHandlingStatus] =
    if (order.amount <= 0) {
      Future.successful(SagaSucceded(MoneyTransferFailed(List("Transfer amount must be positive"))))
    } else if (order.from == order.to) {
      Future.successful(SagaSucceded(MoneyTransferFailed(List("Cannot transfer to the same account"))))
    } else {
      (accountsCommandBus ? Withdraw(Some(sagaStep), order.userId, order.from, order.amount))
        .mapTo[CustomCommandResponse[_]]
        .map {
          case _: SuccessResponse =>
            SagaContinues(CreditTarget(order.userId, order.from, order.to, order.amount))
          case f: FailureResponse =>
            // Nothing happened yet, so there is nothing to compensate — report and stop.
            SagaSucceded(MoneyTransferFailed(f.exceptions))
        }
    }

  private def creditTarget(sagaStep: SagaStep, order: CreditTarget): Future[SagaHandlingStatus] =
    (accountsCommandBus ? Deposit(Some(sagaStep), order.userId, order.to, order.amount))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case _: SuccessResponse =>
          SagaSucceded(MoneyTransferred(order.from, order.to, order.amount))
        case f: FailureResponse =>
          // The source is already debited. Fail, which sends us into REVERTING.
          SagaFailed(MoneyTransferFailed(f.exceptions))
      }

  /** Compensation for a failed credit: give the money back to the source account. */
  private def refundSource(sagaStep: SagaStep, order: CreditTarget): Future[SagaRevertHandlingStatus] =
    (accountsCommandBus ? Deposit(Some(sagaStep), order.userId, order.from, order.amount))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case _: SuccessResponse => SagaRevertSucceded
        case f: FailureResponse => SagaRevertFailed(f.exceptions)
      }
}
