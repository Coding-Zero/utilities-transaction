package com.codingzero.utilities.transaction;

import com.codingzero.utilities.transaction.manager.DefaultTransactionManagerBuilder;

public abstract class TransactionManagerBuilder {

    protected TransactionManagerBuilder() {

    }

    public static TransactionManagerBuilder create() {
        return new DefaultTransactionManagerBuilder();
    }

    abstract public TransactionManager build();

}
