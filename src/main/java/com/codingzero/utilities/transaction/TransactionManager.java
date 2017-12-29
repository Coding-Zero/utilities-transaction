package com.codingzero.utilities.transaction;

/**
 * An interface provides client to register/deregister <tt>TransactionalService</tt>s for a transaction.
 *
 * @param <T>
 */
public interface TransactionManager<T extends TransactionalService> extends Transaction {

    void register(String name, T service);

    T deregister(String name);

}
