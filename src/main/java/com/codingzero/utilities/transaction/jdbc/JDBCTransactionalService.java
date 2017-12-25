package com.codingzero.utilities.transaction.jdbc;

import com.codingzero.utilities.transaction.TransactionContext;
import com.codingzero.utilities.transaction.TransactionalService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class JDBCTransactionalService implements TransactionalService {

    private static final String TRANSACTION_HELPER = "TRANSACTION_HELPER_" + new Object().hashCode();

    private DataSource dataSource;
    private TransactionHelperProvider helperProvider;
    private boolean isGlobalTransactionStarted;
    private boolean isLocalTransactionStarted;
    private TransactionHelper localTransactionHelper;
    private TransactionHelper globalTransactionHelper;
    private TransactionCount sameServiceTransactionCount;

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
        return isGlobalTransactionStarted;
    }

    private void markGlobalTransactionStarted(TransactionContext context) {
        if (isGlobalTransactionStarted()) {
            throw new IllegalStateException("Global transaction already started");
        }
        isGlobalTransactionStarted = true;
        globalTransactionHelper =
                (TransactionHelper) context.getProperty(TRANSACTION_HELPER);
        if (null == globalTransactionHelper) {
            globalTransactionHelper = getHelperProvider().get(getDataSource());
            context.setProperty(TRANSACTION_HELPER, globalTransactionHelper);
        }
    }

    private void cleanGlobalTransactionStartedMark() {
        isGlobalTransactionStarted = false;
        globalTransactionHelper = null;
    }

    private TransactionHelper getGlobalTransactionHelper() {
        return globalTransactionHelper;
    }

    private boolean isLocalTransactionStarted() {
        return isLocalTransactionStarted;
    }

    private void markLocalTransactionStarted() {
        if (isLocalTransactionStarted()) {
            return;
        }
        isLocalTransactionStarted = true;
        if (isGlobalTransactionStarted()) {
            localTransactionHelper = getGlobalTransactionHelper();
        } else {
            localTransactionHelper = getHelperProvider().get(getDataSource());
        }
    }

    private void cleanLocalTransactionStartedMark() {
        isLocalTransactionStarted = false;
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

    protected void startLocalTransaction() {
        markLocalTransactionStarted();
        TransactionHelper helper = getLocalTransactionHelper();
        helper.startTransaction();
        getSameServiceTransactionCount().start();
    }

    protected void commitLocalTransaction() {
        checkForLocalTransactionNotStarted();
        TransactionHelper helper = getLocalTransactionHelper();
        helper.commit();
        getSameServiceTransactionCount().commit();
        if (!helper.isTransactionStarted()) {
            cleanLocalTransactionStartedMark();
        }
    }

    protected void rollbackLocalTransaction() {
        checkForLocalTransactionNotStarted();
        TransactionHelper helper = getLocalTransactionHelper();
        helper.rollback();
        getSameServiceTransactionCount().rollback();
        if (!helper.isTransactionStarted()) {
            cleanLocalTransactionStartedMark();
        }
    }

    @Override
    public void onRegister(String name, TransactionContext context) {
        checkForNullContext(context);
        initSameServiceTransactionCount(context);
    }

    private void initSameServiceTransactionCount(TransactionContext context) {
        String propertyKey = getClass().getCanonicalName();
        sameServiceTransactionCount = (TransactionCount) context.getProperty(propertyKey);
        if (null == sameServiceTransactionCount) {
            sameServiceTransactionCount = new TransactionCount();
            context.setProperty(propertyKey, sameServiceTransactionCount);
        }
    }

    private TransactionCount getSameServiceTransactionCount() {
        return sameServiceTransactionCount;
    }

    @Override
    public void onStartTransaction(TransactionContext context) {
        checkForNullContext(context);
        checkForLocalTransactionStarted();
        markGlobalTransactionStarted(context);
        TransactionHelper helper = getGlobalTransactionHelper();
        helper.startTransaction();
    }

    @Override
    public void onCommitTransaction(TransactionContext context) {
        checkForNullContext(context);
        checkForGlobalTransactionNotStarted();
        TransactionHelper helper = getGlobalTransactionHelper();
        helper.commit();
        checkForUnmatchLocalTransactionCalls();
        if (!helper.isTransactionStarted()) {
            cleanGlobalTransactionStartedMark();
        }
    }

    private void checkForUnmatchLocalTransactionCalls() {
        if (!getSameServiceTransactionCount().isLastCall()
                && isLocalTransactionStarted()) {
            throw new IllegalStateException("Local transaction is not handled properly. "
                    + "Ensure commitLocalTransaction() or rollbackLocalTransaction() get invoked "
                    + "for each startLocalTransaction() call.");
        }
    }

    @Override
    public void onRollbackTransaction(TransactionContext context) {
        checkForNullContext(context);
        checkForGlobalTransactionNotStarted();
        TransactionHelper helper = getGlobalTransactionHelper();
        helper.rollback();
        if (!helper.isTransactionStarted()) {
            cleanGlobalTransactionStartedMark();
        }
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
