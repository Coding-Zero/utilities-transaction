package com.codingzero.utilities.transaction;

public interface TransactionManager<T extends TransactionalService> {

    void register(String name, T service);

    T deregister(String name);

    void startTransaction();

    void commit();

    void rollback();

}
