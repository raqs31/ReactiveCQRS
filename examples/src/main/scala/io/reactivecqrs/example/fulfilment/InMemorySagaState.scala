package io.reactivecqrs.example.fulfilment

import java.util.concurrent.ConcurrentHashMap

import io.reactivecqrs.api.id.{SagaId, UserId}
import io.reactivecqrs.core.saga.{CONTINUES, SagaInternalOrder, SagaPhase, SagaState}

import scala.jdk.CollectionConverters._

/**
 * An in-memory [[SagaState]], so the sample can run with `--in-memory` and no database at all.
 *
 * The framework ships `Memory*` variants of every other durable state — event store, event bus,
 * subscriptions, command responses, type names, uid generator, document store — but **not** of
 * `SagaState`. This fills that gap for the example; it is not part of `core`.
 *
 * Semantics deliberately mirror [[io.reactivecqrs.core.saga.PostgresSagaState]]:
 *
 *  - `updateSaga` changes only the order, phase and step, preserving `respondTo` from the existing
 *    row — the SQL version updates those same columns and leaves `respond_to` alone.
 *  - A missing row on update is ignored rather than raising, matching the SQL version, which
 *    discards its affected-row count.
 *
 * Two differences from the Postgres version that matter when you interpret a run:
 *
 *  - **Sagas do not survive a restart.** Crash-resumption is the main reason saga progress is
 *    persisted at all, so that behaviour cannot be demonstrated in this mode.
 *  - **Saga orders are not serialized.** The Postgres version round-trips every order through
 *    mpjsons, so a serialization problem in an internal order surfaces there and not here. Run at
 *    least once against Postgres before trusting a saga's order shapes.
 */
class InMemorySagaState extends SagaState {

  private case class SagaRow(userId: UserId,
                             respondTo: String,
                             order: SagaInternalOrder,
                             phase: SagaPhase,
                             step: Int)

  private val rows = new ConcurrentHashMap[(String, Long), SagaRow]()

  override def createSaga(name: String, sagaId: SagaId, respondTo: String, order: SagaInternalOrder): Unit = {
    rows.put((name, sagaId.asLong), SagaRow(order.userId, respondTo, order, CONTINUES, 0))
  }

  override def updateSaga(name: String, sagaId: SagaId, order: SagaInternalOrder, phase: SagaPhase, step: Int): Unit = {
    val key = (name, sagaId.asLong)
    Option(rows.get(key)).foreach { existing =>
      rows.put(key, existing.copy(order = order, phase = phase, step = step))
    }
  }

  override def deleteSaga(name: String, sagaId: SagaId): Unit = {
    rows.remove((name, sagaId.asLong))
    ()
  }

  override def loadAllSagas(name: String,
                            handler: (SagaId, UserId, String, SagaPhase, Int, SagaInternalOrder) => Unit): Unit = {
    rows.asScala.toVector
      .filter { case ((rowName, _), _) => rowName == name }
      .foreach { case ((_, sagaId), row) =>
        handler(SagaId(sagaId), row.userId, row.respondTo, row.phase, row.step, row.order)
      }
  }
}
