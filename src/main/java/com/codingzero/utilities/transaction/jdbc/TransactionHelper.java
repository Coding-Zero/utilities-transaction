package com.codingzero.utilities.transaction.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class TransactionHelper {

    private Connection connection;
    private TransactionCount transactionCount;

    public TransactionHelper(Connection connection) {
        setConnection(connection);
        this.transactionCount = new TransactionCount();
    }

    public TransactionCount getTransactionCount() {
        return transactionCount;
    }

    private void setConnection(Connection connection) {
        try {
            this.connection = connection;
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isTransactionStarted() {
        return transactionCount.getStarts() > 0;
    }

    public Connection getConnection() {
        return connection;
    }

    public void startTransaction() {
        transactionCount.start();
    }

    public void commit() {
        checkForTransactionNotStarted();
        transactionCount.commit();
        if (!transactionCount.isLastCall()) {
            return;
        }
        try {
            connection.commit();
            cleanTransaction();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        checkForTransactionNotStarted();
        transactionCount.rollback();
        if (!transactionCount.isLastCall()) {
            return;
        }
        try {
            connection.rollback();
            cleanTransaction();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkForTransactionNotStarted() {
        if (!isTransactionStarted()) {
            throw new IllegalStateException("Need to start a transaction first!");
        }
    }

    private void cleanTransaction() throws SQLException {
        connection.setAutoCommit(true);
        connection.close();
        connection = null;
        transactionCount = new TransactionCount();
    }

}
