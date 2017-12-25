package com.codingzero.utilities.transaction;

public interface TransactionalService {

    void doRegister(String name, TransactionContext context);

    void doStartTransaction(TransactionContext context);

    void doCommit(TransactionContext context);

    void doRollback(TransactionContext context);

}
