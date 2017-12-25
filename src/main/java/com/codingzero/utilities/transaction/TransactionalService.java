package com.codingzero.utilities.transaction;

public interface TransactionalService {

    void onRegister(String name, TransactionContext context);

    void onStartTransaction(TransactionContext context);

    void onCommitTransaction(TransactionContext context);

    void onRollbackTransaction(TransactionContext context);

}
