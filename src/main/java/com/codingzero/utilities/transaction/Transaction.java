package com.codingzero.utilities.transaction;

/**
 * This interface provides client code to control a transaction.
 *
 */
public interface Transaction {

    void start();

    void commit();

    void rollback();

}
