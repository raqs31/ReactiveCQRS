package io.reactivecqrs.example.bank

/**
 * Event handlers rebuild aggregate state. They run both when a command produces an event and
 * on every replay of the stream, so they must be:
 *
 *  - **total** — never throw, never validate. Validation belongs in the command handler; by the
 *    time an event exists, it is a fact.
 *  - **pure** — no I/O, no clock, no random. Given the same events they must produce the same
 *    state, or history stops being reproducible.
 */
object EventHandlers {

  def accountOpened(event: AccountOpened): BankAccount =
    BankAccount(event.owner, event.initialBalance)

  def moneyDeposited(account: BankAccount, event: MoneyDeposited): BankAccount =
    account.copy(balance = account.balance + event.amount)

  def moneyWithdrawn(account: BankAccount, event: MoneyWithdrawn): BankAccount =
    account.copy(balance = account.balance - event.amount)

  def ownerRenamed(account: BankAccount, event: OwnerRenamed): BankAccount =
    account.copy(owner = event.owner)
}
