package io.reactivecqrs.example.bank

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import io.mpjsons.MPJsons
import io.reactivecqrs.api._
import io.reactivecqrs.api.id.{AggregateId, UserId}
import io.reactivecqrs.core.commandhandler.{AggregateCommandBusActor, PostgresCommandResponseState}
import io.reactivecqrs.core.documentstore.{NoopDocumentStoreCache, PostgresDocumentStore}
import io.reactivecqrs.core.eventbus.{EventBusSubscriptionsManager, EventBusSubscriptionsManagerApi, EventsBusActor, PostgresEventBusState}
import io.reactivecqrs.core.eventstore.PostgresEventStoreState
import io.reactivecqrs.core.projection.PostgresSubscriptionsState
import io.reactivecqrs.core.saga.PostgresSagaState
import io.reactivecqrs.core.types.PostgresTypesNamesState
import io.reactivecqrs.core.uid.{PostgresUidGenerator, UidGeneratorActor}
import io.reactivecqrs.example.bank.AccountsProjection.{GetAllAccounts, GetAllActivity}
import io.reactivecqrs.example.bank.MoneyTransferSaga.{MoneyTransferResponse, TransferMoney}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

/**
 * A complete, minimal ReactiveCQRS system: connection pool, schemas, id generator, event bus,
 * command bus, projections, saga — then a short scenario exercising them.
 *
 * Prerequisites: a PostgreSQL database. See `docs/getting-started.md`. Override the connection
 * with `-Dbank.jdbcUrl=... -Dbank.dbUser=... -Dbank.dbPassword=...`.
 *
 * Run with: `sbt "examples/runMain io.reactivecqrs.example.bank.BankExampleApp"`
 */
object BankExampleApp {

  def main(args: Array[String]): Unit = {
    val system = BankSystem.start()
    try {
      runScenario(system)
    } finally {
      Await.result(system.actorSystem.terminate(), 30.seconds)
    }
  }

  private def runScenario(bank: BankSystem): Unit = {
    implicit val timeout: Timeout = Timeout(30.seconds)
    val userId = UserId(1L)

    // Named `askFor`, not `ask`: a local `ask` would shadow Pekko's `ask` implicit conversion
    // that the `?` operator relies on. Blocking like this is fine in a demo, never in an actor.
    def askFor[T: ClassTag](target: ActorRef, message: Any): T =
      Await.result((target ? message).mapTo[T], 30.seconds)

    def accountOf(id: AggregateId): BankAccount =
      askFor[Try[Aggregate[BankAccount]]](bank.accountsCommandBus, GetAggregate(id)).get.aggregateRoot.get

    // --- Write side ------------------------------------------------------------------------

    // A FirstCommand: the bus allocates the AggregateId for us and returns it in the response.
    val alice = askFor[CustomCommandResponse[_]](bank.accountsCommandBus, OpenAccount(None, userId, "Alice", 10_000L))
      .asInstanceOf[SuccessResponse]
    val bob = askFor[CustomCommandResponse[_]](bank.accountsCommandBus, OpenAccount(None, userId, "Bob", 2_500L))
      .asInstanceOf[SuccessResponse]

    println(s"Opened Alice=${alice.aggregateId.asLong} at ${alice.aggregateVersion} and Bob=${bob.aggregateId.asLong}")

    // A ConcurrentCommand: no expected version, retried automatically if it loses a race.
    askFor[CustomCommandResponse[_]](bank.accountsCommandBus, Deposit(None, userId, alice.aggregateId, 500L))

    // A strict Command: we assert the version we based the decision on. Getting this wrong
    // returns AggregateConcurrentModificationError and persists nothing.
    val aliceNow = askFor[Try[Aggregate[BankAccount]]](bank.accountsCommandBus, GetAggregate(alice.aggregateId)).get
    askFor[CustomCommandResponse[_]](bank.accountsCommandBus, RenameOwner(userId, alice.aggregateId, aliceNow.version, "Alice Smith"))

    println(s"Alice after deposit and rename: ${accountOf(alice.aggregateId)}")

    // A rejected command: the failure is a value, not an exception.
    askFor[CustomCommandResponse[_]](bank.accountsCommandBus, Withdraw(None, userId, bob.aggregateId, 999_999L)) match {
      case FailureResponse(reasons) => println(s"Overdraft correctly rejected: ${reasons.mkString(", ")}")
      case other                    => println(s"Unexpected: $other")
    }

    // --- Saga ------------------------------------------------------------------------------

    val transfer = askFor[MoneyTransferResponse](bank.transferSaga, TransferMoney(userId, alice.aggregateId, bob.aggregateId, 1_000L))
    println(s"Transfer result: $transfer")
    println(s"Alice=${accountOf(alice.aggregateId)} Bob=${accountOf(bob.aggregateId)}")

    // --- Time travel -----------------------------------------------------------------------

    val v1 = askFor[Try[Aggregate[BankAccount]]](bank.accountsCommandBus, GetAggregateForVersion(alice.aggregateId, AggregateVersion(1))).get
    println(s"Alice as of version 1: ${v1.aggregateRoot}")

    // --- Read side -------------------------------------------------------------------------

    // Projections are eventually consistent. A production reader would poll or subscribe;
    // the example just waits, which is fine for a demo and wrong for anything else.
    Thread.sleep(1000)

    println(s"Summary projection: ${askFor[Map[AggregateId, AccountSummary]](bank.summaryProjection, GetAllAccounts)}")
    println(s"Activity projection: ${askFor[Map[AggregateId, AccountActivity]](bank.activityProjection, GetAllActivity)}")
  }
}

/**
 * The wiring, extracted so tests and the demo share it.
 *
 * Order matters: the ScalikeJDBC connection pool must exist before any `*State` is constructed,
 * because their constructors and `initSchema()` calls talk to the database immediately.
 */
class BankSystem private (val actorSystem: ActorSystem,
                          val accountsCommandBus: ActorRef,
                          val transferSaga: ActorRef,
                          val summaryProjection: ActorRef,
                          val activityProjection: ActorRef)

object BankSystem {

  def start(): BankSystem = {

    // 1. Connection pool. ReactiveCQRS uses the *default singleton* ScalikeJDBC pool and does
    //    not manage it — the host application must initialise it first.
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(
      sys.props.getOrElse("bank.jdbcUrl", "jdbc:postgresql://localhost:5432/reactivecqrs"),
      sys.props.getOrElse("bank.dbUser", "reactivecqrs"),
      sys.props.getOrElse("bank.dbPassword", "reactivecqrs"),
      ConnectionPoolSettings(initialSize = 5, maxSize = 20, connectionTimeoutMillis = 3000L))

    val system = ActorSystem("bank-example")

    // 2. Serialization and the type-name registry shared by every store.
    val mpjsons = new MPJsons
    val typesNamesState = new PostgresTypesNamesState().initSchema()

    // 3. Durable state. Every `initSchema()` is idempotent CREATE ... IF NOT EXISTS, so it is
    //    safe to call on every boot.
    val eventStoreState = new PostgresEventStoreState(mpjsons, typesNamesState).initSchema()
    val commandResponseState = new PostgresCommandResponseState(mpjsons, typesNamesState).initSchema()
    val eventBusState = new PostgresEventBusState().initSchema()
    val subscriptionsState = new PostgresSubscriptionsState(typesNamesState, keepInMemory = true).initSchema()
    val sagaState = new PostgresSagaState(mpjsons, typesNamesState)
    sagaState.initSchema()

    // 4. Id generation. Each generator reads a Postgres sequence and hands out pools of ids;
    //    the sequence's `increment_by` is the pool size, so raise it if id allocation is hot.
    val uidGenerator = system.actorOf(Props(new UidGeneratorActor(
      new PostgresUidGenerator("aggregates_uids_seq"),
      new PostgresUidGenerator("commands_uids_seq"),
      new PostgresUidGenerator("sagas_uids_seq"))), "uidGenerator")

    // 5. Event bus. The constructor argument is how many subscribers to expect before the bus
    //    starts publishing — count your projections. Get it wrong and events sit undelivered
    //    (too high) or projections miss the earliest events (too low).
    val expectedSubscribers = 2
    val eventBusSubscriptionsManager =
      new EventBusSubscriptionsManagerApi(system.actorOf(Props(new EventBusSubscriptionsManager(expectedSubscribers))))
    val eventBusActor = system.actorOf(Props(new EventsBusActor(eventBusState, eventBusSubscriptionsManager)), "eventBus")

    // 6. One command bus per aggregate type.
    val accountsCommandBus = system.actorOf(
      AggregateCommandBusActor(new BankAccountAggregateContext, uidGenerator, eventStoreState,
        commandResponseState, eventBusActor, eventsReplayMode = false),
      "BankAccountCommandBus")

    // 7. Read models. Each projection owns its own document store table.
    val summaryStore = new PostgresDocumentStore[AccountSummary]("accounts_summary", mpjsons, new NoopDocumentStoreCache)
    val activityStore = new PostgresDocumentStore[AccountActivity]("accounts_activity", mpjsons, new NoopDocumentStoreCache)

    val summaryProjection = system.actorOf(
      Props(new AccountsSummaryProjection(eventBusSubscriptionsManager, subscriptionsState, summaryStore)),
      "AccountsSummaryProjection")
    val activityProjection = system.actorOf(
      Props(new AccountsActivityProjection(eventBusSubscriptionsManager, subscriptionsState, activityStore)),
      "AccountsActivityProjection")

    // 8. Sagas.
    val transferSaga = system.actorOf(
      Props(new MoneyTransferSaga(sagaState, uidGenerator, accountsCommandBus)), "MoneyTransferSaga")

    // Projections register with the bus asynchronously on start. Anything published before they
    // subscribe is delivered later from the `events_to_publish` outbox, but the demo prefers a
    // clean start.
    Thread.sleep(200)

    new BankSystem(system, accountsCommandBus, transferSaga, summaryProjection, activityProjection)
  }
}
