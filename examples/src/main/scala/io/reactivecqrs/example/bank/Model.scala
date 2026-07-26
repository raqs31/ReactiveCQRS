package io.reactivecqrs.example.bank

/**
 * The aggregate root: a plain immutable case class.
 *
 * The framework never mutates this — event handlers return a new instance. It is held
 * as `Option[BankAccount]`; `None` (or a handler returning `null`) means "deleted".
 *
 * Balances are in minor units (cents) to keep the example free of decimal concerns.
 */
case class BankAccount(owner: String, balance: Long)
