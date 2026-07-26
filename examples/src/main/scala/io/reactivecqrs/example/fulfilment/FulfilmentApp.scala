package io.reactivecqrs.example.fulfilment

import java.util.concurrent.{Executors, TimeUnit}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.mpjsons.MPJsons
import io.reactivecqrs.api.id.UserId
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, CommandResponseState, MemoryCommandResponseState, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{DocumentStore, MemoryDocumentStore, NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.{EventBusState, EventBusSubscriptionsManager, EventBusSubscriptionsManagerApi, EventsBusActor, MemoryEventBusState, PostgresEventBusState}
import io.reactivecqrs.core.eventstore.{EventStoreState, MemoryEventStoreState, PostgresEventStoreState}
import io.reactivecqrs.core.projection.{MemorySubscriptionsState, PostgresSubscriptionsState, SubscriptionsState}
import io.reactivecqrs.core.saga.{PostgresSagaState, SagaState}
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{MemoryUidGenerator, PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.example.fulfilment.FulfilmentProjections.{GetOrders, GetShipments, GetStats}
import org.slf4j.LoggerFactory
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

case class FulfilmentConfig(orders: Int = 20,
                            rate: Double = 4.0,
                            seed: Long = 1L,
                            concurrency: Int = 8,
                            reportEvery: Int = 5,
                            inMemory: Boolean = false,
                            jdbcUrl: String = "jdbc:postgresql://localhost:5432/reactivecqrs",
                            dbUser: String = "reactivecqrs",
                            dbPassword: String = "reactivecqrs")

/**
 * An ordering-and-shipping sample application you can run from the command line.
 *
 * It wires two aggregate types (`Order`, `Shipment`), a saga coordinating them, and three
 * projections; then generates randomised order traffic and logs every step so the CQRS pipeline is
 * visible as it happens.
 *
 * {{{
 * sbt "examples/runMain io.reactivecqrs.example.fulfilment.FulfilmentApp --orders 30 --rate 5 --seed 42"
 * }}}
 *
 * See `docs/guides/07-sample-app.md` for a walkthrough of the output.
 */
object FulfilmentApp {

  private val log = LoggerFactory.getLogger("app")

  private val Usage =
    """ReactiveCQRS ordering + shipping sample.
      |
      |Usage: FulfilmentApp [options]
      |
      |  --orders N        number of order flows to generate   (default 20)
      |  --rate N          order flows started per second      (default 4)
      |  --seed N          RNG seed; same seed = same run      (default 1)
      |  --concurrency N   threads generating traffic          (default 8)
      |  --report-every N  seconds between dashboard reports   (default 5, 0 disables)
      |  --in-memory       run with no database at all         (default off)
      |  --jdbc-url URL    JDBC url    (default jdbc:postgresql://localhost:5432/reactivecqrs)
      |  --db-user U       database user     (default reactivecqrs)
      |  --db-password P   database password (default reactivecqrs)
      |  --help            show this message
      |
      |Tip: set the `io.reactivecqrs` logger to DEBUG in examples/src/main/resources/logback.xml
      |     to see the framework's own actor traffic alongside the application log.
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    parse(args.toList, FulfilmentConfig()) match {
      case None =>
        println(Usage)
      case Some(config) =>
        val system = FulfilmentSystem.start(config)
        try run(system, config)
        finally {
          log.info("shutting down")
          Await.result(system.actorSystem.terminate(), 30.seconds)
        }
    }
  }

  private def parse(args: List[String], config: FulfilmentConfig): Option[FulfilmentConfig] = args match {
    case Nil                              => Some(config)
    case "--help" :: _                    => None
    case "--orders" :: v :: rest          => parse(rest, config.copy(orders = v.toInt))
    case "--rate" :: v :: rest            => parse(rest, config.copy(rate = v.toDouble))
    case "--seed" :: v :: rest            => parse(rest, config.copy(seed = v.toLong))
    case "--concurrency" :: v :: rest     => parse(rest, config.copy(concurrency = v.toInt))
    case "--report-every" :: v :: rest    => parse(rest, config.copy(reportEvery = v.toInt))
    case "--in-memory" :: rest            => parse(rest, config.copy(inMemory = true))
    case "--jdbc-url" :: v :: rest        => parse(rest, config.copy(jdbcUrl = v))
    case "--db-user" :: v :: rest         => parse(rest, config.copy(dbUser = v))
    case "--db-password" :: v :: rest     => parse(rest, config.copy(dbPassword = v))
    case unknown :: _ =>
      println(s"Unrecognised argument: $unknown")
      None
  }

  private def run(sys: FulfilmentSystem, config: FulfilmentConfig): Unit = {
    val pool = Executors.newFixedThreadPool(config.concurrency)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)

    val reporter = Executors.newSingleThreadScheduledExecutor()
    if (config.reportEvery > 0) {
      reporter.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = try report(sys, "progress") catch { case NonFatal(_) => () }
      }, config.reportEvery.toLong, config.reportEvery.toLong, TimeUnit.SECONDS)
    }

    val generator = new TrafficGenerator(sys.ordersBus, sys.shipmentsBus, sys.fulfilSaga, 60.seconds)
    val userId = UserId(1L)
    val intervalMillis = math.max(1L, (1000.0 / config.rate).toLong)

    log.info(s"generating ${config.orders} order flows at ~${config.rate}/s (seed=${config.seed}, concurrency=${config.concurrency})")
    val started = System.nanoTime()

    // Pacing happens on this thread; each flow runs on the pool, so flows genuinely overlap and the
    // aggregates see concurrent traffic.
    val flows: Seq[Future[String]] = (1 to config.orders).map { i =>
      if (i > 1) Thread.sleep(intervalMillis)
      Future {
        try generator.runOrder(i, config.seed, userId)
        catch { case NonFatal(e) => log.error(s"order flow #$i failed: ${e.getMessage}", e); "error" }
      }
    }

    val outcomes = Await.result(Future.sequence(flows), 30.minutes)
    val elapsedMillis = (System.nanoTime() - started) / 1000000L

    reporter.shutdownNow()
    pool.shutdown()

    log.info("-" * 78)
    log.info(s"all ${config.orders} flows finished in ${elapsedMillis}ms")
    outcomes.groupBy(identity).view.mapValues(_.size).toVector.sortBy(-_._2).foreach {
      case (outcome, count) => log.info(f"  $outcome%-26s $count%4d")
    }

    // Projections are eventually consistent: the flows are done, but the event bus may still be
    // delivering. Give it a moment before reading the read models.
    log.info("waiting for projections to catch up...")
    Thread.sleep(3000)
    report(sys, "final")
  }

  private def report(sys: FulfilmentSystem, label: String): Unit = {
    implicit val timeout: Timeout = Timeout(30.seconds)

    val orders = Await.result((sys.ordersProjection ? GetOrders).mapTo[Map[Long, OrderSummary]], 30.seconds)
    val shipments = Await.result((sys.shipmentsProjection ? GetShipments).mapTo[Map[Long, ShipmentSummary]], 30.seconds)
    val stats = Await.result((sys.statsProjection ? GetStats).mapTo[Option[FulfilmentStats]], 30.seconds)

    log.info("=" * 78)
    log.info(s"READ MODELS ($label)")
    log.info(s"  orders    : ${orders.size} " + orders.values.groupBy(_.status).map { case (s, v) => s"$s=${v.size}" }.mkString("[", " ", "]"))
    log.info(s"  shipments : ${shipments.size} " + shipments.values.groupBy(_.status).map { case (s, v) => s"$s=${v.size}" }.mkString("[", " ", "]"))
    stats.foreach { s =>
      log.info(s"  events    : placed=${s.ordersPlaced} paid=${s.ordersPaid} shipped=${s.ordersShipped} cancelled=${s.ordersCancelled}")
      log.info(s"              shipments created=${s.shipmentsCreated} dispatched=${s.shipmentsDispatched} cancelled=${s.shipmentsCancelled}")
    }
    log.info("=" * 78)
  }
}

/** The assembled system. Kept separate from the runner so tests could reuse the wiring. */
class FulfilmentSystem private (val actorSystem: ActorSystem,
                                val ordersBus: ActorRef,
                                val shipmentsBus: ActorRef,
                                val fulfilSaga: ActorRef,
                                val ordersProjection: ActorRef,
                                val shipmentsProjection: ActorRef,
                                val statsProjection: ActorRef)

object FulfilmentSystem {

  private val log = LoggerFactory.getLogger("app")

  def start(config: FulfilmentConfig): FulfilmentSystem = {

    val system = ActorSystem("fulfilment-example")
    val mpjsons = new MPJsons

    // Everything below is chosen once, here. The rest of the wiring is identical in both modes,
    // because the actors depend on the `*State` abstractions rather than on their implementations.
    val (eventStoreState, commandResponseState, eventBusState, subscriptionsState, sagaState, uidGenerator) =
      if (config.inMemory) {
        log.info("running fully in memory - no database, nothing survives this process")
        (new MemoryEventStoreState: EventStoreState,
          new MemoryCommandResponseState: CommandResponseState,
          new MemoryEventBusState: EventBusState,
          new MemorySubscriptionsState: SubscriptionsState,
          new InMemorySagaState: SagaState,
          system.actorOf(Props(new UidGeneratorActor(
            new MemoryUidGenerator, new MemoryUidGenerator, new MemoryUidGenerator)), "uidGenerator"))
      } else {
        log.info(s"connecting to ${config.jdbcUrl} as ${config.dbUser}")
        Class.forName("org.postgresql.Driver")
        ConnectionPool.singleton(config.jdbcUrl, config.dbUser, config.dbPassword,
          ConnectionPoolSettings(initialSize = 5, maxSize = 30, connectionTimeoutMillis = 5000L))

        val typesNamesState = new PostgresTypesNamesState().initSchema()
        val sagas = new PostgresSagaState(mpjsons, typesNamesState)
        sagas.initSchema()
        (new PostgresEventStoreState(mpjsons, typesNamesState).initSchema(): EventStoreState,
          new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema(): CommandResponseState,
          new PostgresEventBusState().initSchema(): EventBusState,
          new PostgresSubscriptionsState(typesNamesState, keepInMemory = true).initSchema(): SubscriptionsState,
          sagas: SagaState,
          system.actorOf(Props(new UidGeneratorActor(
            new PostgresUidGenerator("aggregates_uids_seq"),
            new PostgresUidGenerator("commands_uids_seq"),
            new PostgresUidGenerator("sagas_uids_seq"))), "uidGenerator"))
      }

    // Three projection actors subscribe, so the bus waits for three before it starts publishing.
    val expectedSubscribers = 3
    val eventBusSubscriptionsManager =
      new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(expectedSubscribers))))
    val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState, eventBusSubscriptionsManager)), "eventBus")

    // One command bus per aggregate type.
    val ordersBus = system.actorOf(
      AggregateCommandBusActor(new OrderAggregateContext, uidGenerator, eventStoreState,
        commandResponseState, eventBusActor, eventsReplayMode = false), "OrderCommandBus")
    val shipmentsBus = system.actorOf(
      AggregateCommandBusActor(new ShipmentAggregateContext, uidGenerator, eventStoreState,
        commandResponseState, eventBusActor, eventsReplayMode = false), "ShipmentCommandBus")

    // Projections are written against `DocumentStore[T]`, so the same projection classes work with
    // either backing store — no conditional code inside the projections themselves.
    val ordersStore: DocumentStore[OrderSummary] =
      if (config.inMemory) new MemoryDocumentStore[OrderSummary]
      else new PostgresDocumentStore[OrderSummary]("fulfilment_orders", mpjsons, new NoopDocumentStoreCache)
    val shipmentsStore: DocumentStore[ShipmentSummary] =
      if (config.inMemory) new MemoryDocumentStore[ShipmentSummary]
      else new PostgresDocumentStore[ShipmentSummary]("fulfilment_shipments", mpjsons, new NoopDocumentStoreCache)
    val statsStore: DocumentStore[FulfilmentStats] =
      if (config.inMemory) new MemoryDocumentStore[FulfilmentStats]
      else new PostgresDocumentStore[FulfilmentStats]("fulfilment_stats", mpjsons, new NoopDocumentStoreCache)

    val ordersProjection = system.actorOf(
      Props(new OrdersProjection(eventBusSubscriptionsManager, subscriptionsState, ordersStore)), "OrdersProjection")
    val shipmentsProjection = system.actorOf(
      Props(new ShipmentsProjection(eventBusSubscriptionsManager, subscriptionsState, shipmentsStore)), "ShipmentsProjection")
    val statsProjection = system.actorOf(
      Props(new FulfilmentStatsProjection(eventBusSubscriptionsManager, subscriptionsState, statsStore)), "StatsProjection")

    val fulfilSaga = system.actorOf(
      Props(new FulfilOrderSaga(sagaState, uidGenerator, ordersBus, shipmentsBus)), "FulfilOrderSaga")

    // Projections register asynchronously; let them settle before generating traffic.
    Thread.sleep(500)
    log.info("system ready")

    new FulfilmentSystem(system, ordersBus, shipmentsBus, fulfilSaga,
      ordersProjection, shipmentsProjection, statsProjection)
  }
}
