package com.codingzero.utilities.transaction;

/**
 * An interface provides client to register/deregister <tt>TransactionalService</tt>s for a transaction.
 */
public interface TransactionManager extends Transaction {

    void register(String name, TransactionalService service);

    TransactionalService deregister(String name);

}
