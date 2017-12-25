package com.codingzero.utilities.transaction.jdbc;

import javax.sql.DataSource;
import java.sql.SQLException;

public class TransactionHelperProvider {

    public TransactionHelper get(DataSource dataSource) {
        try {
            return new TransactionHelper(dataSource.getConnection());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
