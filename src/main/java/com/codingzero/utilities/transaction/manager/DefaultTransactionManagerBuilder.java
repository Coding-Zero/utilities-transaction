package com.codingzero.utilities.transaction.manager;

import com.codingzero.utilities.transaction.TransactionManager;
import com.codingzero.utilities.transaction.TransactionManagerBuilder;

public class DefaultTransactionManagerBuilder extends TransactionManagerBuilder {

    @Override
    public TransactionManager build() {
        return new TransactionManagerImpl();
    }

}
