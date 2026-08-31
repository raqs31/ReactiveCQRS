# Example: Shopping Cart

The `testdomain` module is the canonical worked example
(`testdomain/src/main/scala/io/reactivecqrs/testdomain/shoppingcart/`). This walkthrough
covers the whole surface: aggregate, commands, events, handlers, projections, a saga,
undo, duplication, and history rewrite.

## 1. Aggregate root — a plain case class

```scala
case class ShoppingCart(name: String, items: Vector[Item])
case class Item(id: Int, name: String)
```

## 2. Events

```scala
case class ShoppingCartCreated(name: String) extends FirstEvent[ShoppingCart] {
  def spaceId: SpaceId = SpaceId(0)
}
case class ItemAdded(name: String)  extends Event[ShoppingCart]
case class ItemRemoved(id: Int)     extends Event[ShoppingCart]
case class ShoppingCartDeleted()    extends Event[ShoppingCart]

// special events
case class ShoppingCartChangesUndone(eventsCount: Int) extends UndoEvent[ShoppingCart]
case class ShoppingCartDuplicated(spaceId: SpaceId, baseAggregateId: AggregateId,
                                  baseAggregateVersion: AggregateVersion) extends DuplicationEvent[ShoppingCart]
```

## 3. Commands

```scala
// FirstCommand: creates the aggregate; idempotent when idempotencyId is Some
case class CreateShoppingCart(idempotencyId: Option[SagaStep], userId: UserId, name: String)
  extends FirstCommand[ShoppingCart, CustomCommandResponse[_]] with IdempotentCommand[SagaStep]

// Command: optimistic lock via expectedVersion
case class AddItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                   name: String) extends Command[ShoppingCart, CustomCommandResponse[_]]

case class UndoShoppingCartChange(userId: UserId, aggregateId: AggregateId,
                                  expectedVersion: AggregateVersion, stepsToUndo: Int)
  extends Command[ShoppingCart, CustomCommandResponse[_]]

// Rewrites past ShoppingCartCreated events
case class RewriteCartName(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
                           name: String) extends RewriteHistoryCommand[ShoppingCart, CustomCommandResponse[_]] {
  override def eventsTypes = Set(classOf[ShoppingCartCreated])
}
```

## 4. The AggregateContext

```scala
class ShoppingCartAggregateContext extends AggregateContext[ShoppingCart] {

  override val version: Int = 1
  override val eventsVersions = EV[ShoppingCartCreated](0 -> classOf[ShoppingCartCreated]) :: Nil

  override def initialAggregateRoot = ShoppingCart("", Vector())

  override def commandHandlers = shoppingCart => {
    case c: CreateShoppingCart     => createShoppingCart(c.userId, c)
    case c: AddItem                => addItem(c.userId, c.aggregateId, c.expectedVersion, shoppingCart)(c)
    case c: RemoveItem             => removeItem(c)
    case c: DeleteShoppingCart     => deleteShoppingCart(c)
    case c: UndoShoppingCartChange => undoShoppingCartChange(c)
  }

  override def eventHandlers = (userId, timestamp, shoppingCart) => {
    case e: ShoppingCartCreated       => ShoppingCart(e.name, Vector())
    case e: ItemAdded                 => itemAdded(shoppingCart, e)
    case e: ItemRemoved               => shoppingCart.copy(items = shoppingCart.items.filterNot(_.id == e.id))
    case e: ShoppingCartDeleted       => null                // null = aggregate deleted (root becomes None)
    case e: ShoppingCartChangesUndone => shoppingCart        // undo is applied by the framework
    case e: ShoppingCartDuplicated    => shoppingCart
  }
}
```

### Command handlers — sync and async

```scala
// Synchronous
def removeItem(command: RemoveItem) = CommandSuccess(ItemRemoved(command.id))

// Async: return a Future[CustomCommandResult] (implicitly wrapped) or AsyncCommandResult
def addItem(userId: UserId, aggregateId: AggregateId, expectedVersion: AggregateVersion,
            shoppingCart: ShoppingCart)(command: AddItem) =
  AsyncCommandResult(Future {
    if (shoppingCart.items.size > 5) CommandFailure("Cannot have more than 5 items in your cart")
    else CommandSuccess(ItemAdded(command.name))
  })
```

Validation failures are values (`CommandFailure`), not exceptions.

## 5. Projections (read models)

Extend `ProjectionActor`, declare which delivery style you listen to, and write to a
`DocumentStore`. Progress is tracked per aggregate in the `subscriptions` table, so
delivery is exactly-once *per subscription* as long as your handler is idempotent.

### Events-based listener

```scala
class ShoppingCartsListProjectionEventsBased(val eventBusSubscriptionsManager: EventBusSubscriptionsManagerApi,
                                             val subscriptionsState: SubscriptionsState,
                                             shoppingCartCommandBus: ActorRef,
                                             documentStore: DocumentStore[String]) extends ProjectionActor {
  override protected val projectionName = "ShoppingCartsListProjectionEventsBased"
  override protected val version: Int = 1

  protected val listeners = List(EventsListener(shoppingCartUpdate))

  private def shoppingCartUpdate(aggregateId: AggregateId, events: Seq[EventInfo[ShoppingCart]]) =
    { implicit session: DBSession =>
      events.foreach(_.event match {
        case ShoppingCartCreated(name) => documentStore.insertDocument(0, aggregateId.asLong, name)
        case ShoppingCartDeleted()     => documentStore.removeDocument(aggregateId.asLong)
        case _                         => // ...
      })
    }

  // query side — plain actor messages
  override protected def receiveQuery: Receive = {
    case GetAllCartsNames() => sender() ! documentStore.findAll().values.map(_.document).toVector
  }
}
```

### Aggregate-based listener (gets the full state instead of events)

```scala
protected val listeners = List(AggregateListener(shoppingCartUpdate))

private def shoppingCartUpdate(aggregateId: AggregateId, version: AggregateVersion,
                               created: Boolean, aggregateRoot: Option[ShoppingCart]) =
  { implicit session: DBSession =>
    aggregateRoot match {
      case Some(a) if created => documentStore.insertDocument(0, aggregateId.asLong, a.name)
      case Some(a)            => documentStore.overwriteDocument(aggregateId.asLong, a.name)
      case None               => documentStore.removeDocument(aggregateId.asLong)
    }
  }
```

## 6. Saga — multi-step process with compensation

A saga issues commands step by step; each step's `SagaStep(sagaId, step)` is the
idempotency key, so a replayed step can't double-execute. On failure the saga *reverts*
completed steps with compensating commands.

```scala
object MultipleCartCreatorSaga {
  case class CreateMultipleCarts(userId: UserId, cartName: String, cartsCount: Int) extends SagaOrder
  private case class CreateRemainingCarts(userId: UserId, cartName: String, cartsCount: Int,
                                          createdCarts: List[AggregateIdWithVersion]) extends SagaInternalOrder
  case class CartsCreated(carts: List[AggregateIdWithVersion]) extends SagaResponse
  case class CartsCreationFailure(exceptions: List[String]) extends SagaResponse
}

class MultipleCartCreatorSaga(val state: SagaState, val uidGenerator: ActorRef,
                              shoppingCartCommandBus: ActorRef) extends SagaActor {
  override val name = "MultipleCartCreatorSaga"

  override def handleOrder(sagaStep: SagaStep): ReceiveOrder = {
    case order: CreateMultipleCarts  => handleCreateMultipleCarts(sagaStep, order)
    case order: CreateRemainingCarts => handleCreateRemainingCarts(sagaStep, order)
  }

  override def handleRevert(sagaStep: SagaStep): ReceiveRevert = {
    case revert: CreateRemainingCarts => revertCreateRemainingCarts(sagaStep, /* ... */)
  }

  private def createShoppingCart(sagaStep: SagaStep, userId: UserId, cartName: String,
                                 cartsCount: Int, createdCarts: List[AggregateIdWithVersion]) =
    (shoppingCartCommandBus ? CreateShoppingCart(Some(sagaStep), userId, cartName))
      .mapTo[CustomCommandResponse[_]]
      .map {
        case c: SuccessResponse if createdCarts.length + 1 < cartsCount =>
          SagaContinues(CreateRemainingCarts(userId, cartName, cartsCount,
            AggregateIdWithVersion(c.aggregateId, c.aggregateVersion) :: createdCarts))
        case c: SuccessResponse => SagaSucceded(CartsCreated(/* ... */))
        case c: FailureResponse => SagaFailed(CartsCreationFailure(c.exceptions))
      }
}
```

Revert steps issue compensating commands (`DeleteShoppingCart(Some(sagaStep), ...)`) and
return `SagaRevertContinues` / `SagaRevertSucceded` / `SagaRevertFailed`.

## 7. Special operations in action

```scala
// Undo: cancels the last N events logically (version still moves forward)
bus ? UndoShoppingCartChange(userId, cartId, AggregateVersion(4), stepsToUndo = 1)

// Duplication: new aggregate sharing history with the base up to a version
bus ? DuplicateShoppingCart(userId, baseCartId, AggregateVersion(3))
// => new aggregate at version 1, whose state includes the base's first 3 events

// Time travel
bus ? GetAggregateForVersion(cartId, AggregateVersion(2))

// History rewrite: retroactively change past events (e.g. anonymization)
bus ? RewriteCartName(userId, cartId, currentVersion, "anonymized")
```

Full runnable scenarios: `testdomain/src/test/scala/io/reactivecqrs/testdomain/spec/ReactiveTestDomainSpec.scala`
(system wiring at the top, then create/undo/duplicate/rewrite/saga scenarios) and
`EventsReplaySpec.scala` for replaying all events into rebuilt projections.
