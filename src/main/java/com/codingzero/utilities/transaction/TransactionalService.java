package com.codingzero.utilities.transaction;

/**
 * This is an observer for getting notifications for different states of a transaction.
 *
 */
public interface TransactionalService {

    void onRegister(String name, TransactionContext context);

    void onStartTransaction(TransactionContext context);

    void onCommitTransaction(TransactionContext context);

    void onRollbackTransaction(TransactionContext context);

}
