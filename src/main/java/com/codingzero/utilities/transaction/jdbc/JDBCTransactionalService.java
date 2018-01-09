package com.codingzero.utilities.transaction.jdbc;

import com.codingzero.utilities.transaction.Transaction;
import com.codingzero.utilities.transaction.TransactionContext;
import com.codingzero.utilities.transaction.TransactionalService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class JDBCTransactionalService implements TransactionalService, Transaction {

    private static final String TRANSACTION_HELPER = "TRANSACTION_HELPER_" + new Object().hashCode();

    private DataSource dataSource;
    private TransactionHelperProvider helperProvider;
    private TransactionHelper localTransactionHelper; //instance wide
    private TransactionHelper globalTransactionHelper;
    private TransactionCount serviceTransactionCount; //class wide

    public JDBCTransactionalService(DataSource dataSource) {
        this(dataSource, new TransactionHelperProvider());
    }

    public JDBCTransactionalService(DataSource dataSource, TransactionHelperProvider helperProvider) {
        this.dataSource = dataSource;
        this.helperProvider = helperProvider;
        this.localTransactionHelper = null;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public TransactionHelperProvider getHelperProvider() {
        return helperProvider;
    }

    private boolean isGlobalTransactionStarted() {
        if (null == getGlobalTransactionHelper()) {
            return false;
        }
        return globalTransactionHelper.isTransactionStarted();
    }

    private void markGlobalTransactionStarted(TransactionContext context) {
        if (isGlobalTransactionStarted()) {
            throw new IllegalStateException("Global transaction already started");
        }
        globalTransactionHelper =
                (TransactionHelper) context.getProperty(TRANSACTION_HELPER);
        if (null == globalTransactionHelper) {
            globalTransactionHelper = getHelperProvider().get(getDataSource());
            context.setProperty(TRANSACTION_HELPER, globalTransactionHelper);
        }
    }

    private void cleanGlobalTransactionStartedMark() {
        globalTransactionHelper = null;
    }

    private void cleanGlobalTransactionHelper(TransactionContext context) {
        context.removeProperty(TRANSACTION_HELPER);
    }

    private TransactionHelper getGlobalTransactionHelper() {
        return globalTransactionHelper;
    }

    private boolean isLocalTransactionStarted() {
        return null != localTransactionHelper;
    }

    private void markLocalTransactionStarted() {
        if (isLocalTransactionStarted()) {
            return;
        }
        localTransactionHelper = getHelperProvider().get(getDataSource());
    }

    private void cleanLocalTransactionStartedMark() {
        localTransactionHelper = null;
    }

    private TransactionHelper getLocalTransactionHelper() {
        return localTransactionHelper;
    }

    protected Connection getConnection() {
        if (isGlobalTransactionStarted()) {
            return getGlobalTransactionHelper().getConnection();
        } else if (isLocalTransactionStarted()) {
            return getLocalTransactionHelper().getConnection();
        }
        return getNoneTransactionConnection();
    }

    private Connection getNoneTransactionConnection() {
        try {
            return getDataSource().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void closePreparedStatement(PreparedStatement stmt) {
        try {
            if (null != stmt) {
                stmt.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void closeConnection(Connection conn) {
        if (isGlobalTransactionStarted()
                || isLocalTransactionStarted()) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        markLocalTransactionStarted();
        getLocalTransactionHelper().startTransaction();
        getServiceTransactionCount().start();
    }

    @Override
    public void commit() {
        checkForLocalTransactionNotStarted();
        getLocalTransactionHelper().commit();
        getServiceTransactionCount().commit();
        if (!getLocalTransactionHelper().isTransactionStarted()) {
            cleanLocalTransactionStartedMark();
        }
    }

    @Override
    public void rollback() {
        checkForLocalTransactionNotStarted();
        getLocalTransactionHelper().rollback();
        getServiceTransactionCount().rollback();
        if (!getLocalTransactionHelper().isTransactionStarted()) {
            cleanLocalTransactionStartedMark();
        }
    }

    @Override
    public void onRegister(String name, TransactionContext context) {
        checkForNullContext(context);
        setServiceTransactionCountProperty(context);
    }

    private void setServiceTransactionCountProperty(TransactionContext context) {
        String propertyKey = getClass().getCanonicalName();
        serviceTransactionCount = (TransactionCount) context.getProperty(propertyKey);
        if (null == serviceTransactionCount) {
            serviceTransactionCount = new TransactionCount();
            context.setProperty(propertyKey, serviceTransactionCount);
        }
    }

    private TransactionCount getServiceTransactionCount() {
        return serviceTransactionCount;
    }

    @Override
    public void onStartTransaction(TransactionContext context) {
        checkForNullContext(context);
        checkForLocalTransactionStarted();
        markGlobalTransactionStarted(context);
        getGlobalTransactionHelper().startTransaction();
    }

    @Override
    public void onCommitTransaction(TransactionContext context) {
        checkForNullContext(context);
        checkForGlobalTransactionNotStarted();
        getGlobalTransactionHelper().commit();
        checkForMismatchLocalTransactionCalls();
        if (!getGlobalTransactionHelper().isTransactionStarted()) {
            cleanGlobalTransactionHelper(context);
        }
        cleanGlobalTransactionStartedMark();
    }

    /**
     * This is used to ensure each start() has one commit() or rollback() get called after for each instance.
     *
     * service class wide transaction count is used to avoid the mis-calculation
     * happen when the same class got registered more than once.
     */
    private void checkForMismatchLocalTransactionCalls() {
        if (!getServiceTransactionCount().isLastCall()
                && isLocalTransactionStarted()) {
            throw new IllegalStateException("Local transaction is not handled properly. "
                    + "Ensure commit() or rollback() get invoked "
                    + "for each start() call.");
        }
    }

    @Override
    public void onRollbackTransaction(TransactionContext context) {
        checkForNullContext(context);
        checkForGlobalTransactionNotStarted();
        getGlobalTransactionHelper().rollback();
        if (!getGlobalTransactionHelper().isTransactionStarted()) {
            cleanGlobalTransactionHelper(context);
        }
        cleanGlobalTransactionStartedMark();
    }

    private void checkForNullContext(TransactionContext context) {
        if (null == context) {
            throw new IllegalArgumentException("TransactionContext cannot be null value.");
        }
    }

    private void checkForLocalTransactionStarted() {
        if (isLocalTransactionStarted()) {
            throw new IllegalStateException("Global transaction need started before a local transaction");
        }
    }

    private void checkForGlobalTransactionNotStarted() {
        if (!isGlobalTransactionStarted()) {
            throw new IllegalStateException("No global transaction!");
        }
    }

    private void checkForLocalTransactionNotStarted() {
        if (!isLocalTransactionStarted()) {
            throw new IllegalStateException("No local transaction!");
        }
    }

}
