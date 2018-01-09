package com.codingzero.utilities.transaction;

import com.codingzero.utilities.transaction.jdbc.JDBCTransactionalService;
import com.codingzero.utilities.transaction.jdbc.TransactionHelperProvider;
import com.codingzero.utilities.transaction.manager.TransactionManagerImpl;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import static org.junit.Assert.assertEquals;


public class JDBCTransactionServiceTest {

    private static String SCHEMA_NAME = "utilities_transaction";
    private static String TABLE_NAME = "table1";
    private static String CREATE_SCHEMA = "CREATE SCHEMA `" + SCHEMA_NAME + "` DEFAULT CHARACTER SET utf8 ;";
    private static String CREATE_TABLE = "CREATE TABLE `" + SCHEMA_NAME + "`.`" + TABLE_NAME + "` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `value` VARCHAR(35) NULL,\n"
            + "  PRIMARY KEY (`id`));";
    private static String DROP_TABLE = "DROP TABLE `" + SCHEMA_NAME + "`.`" + TABLE_NAME + "`;";
    private static String DROP_SCHEMA = "DROP DATABASE `" + SCHEMA_NAME + "`;";

    private DataSource dataSource;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws SQLException {
        dataSource = getMySQLDataSource();
        runScript(dataSource, CREATE_SCHEMA);
        runScript(dataSource, CREATE_TABLE);
    }

    @After
    public void cleanUp() throws SQLException {
        runScript(dataSource, DROP_TABLE);
        runScript(dataSource, DROP_SCHEMA);
        dataSource = null;
    }

    public static DataSource getMySQLDataSource() {
        MysqlDataSource mysqlDS = new MysqlDataSource();
        mysqlDS.setURL("jdbc:mysql://localhost:3306/");
        mysqlDS.setUser("root");
        mysqlDS.setPassword("123456");
        return mysqlDS;
    }

    public void runScript(DataSource source, String sql) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement stmt=null;
        try {
            stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            stmt.close();
            conn.close();
        }
    }

    @Test
    public void testSingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        String value = access.selectById(1);
        assertEquals(date, value);
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access.count());
        String value = access.selectById(1);
        assertEquals(null, value);
    }

    @Test
    public void testSingleInsert_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        String value = access.selectById(1);
        assertEquals(date, value);
    }

    @Test
    public void testSingleInsert_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("service", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insertWithTransaction(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access.count());
        String value = access.selectById(1);
        assertEquals(null, value);
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        long currentTimestamp = new Date().getTime();
        String date1 = new Date(currentTimestamp).toString();
        String date2 = new Date(currentTimestamp + 1000).toString();
        String date3 = new Date(currentTimestamp + 2000).toString();
        manager.start();
        try {
            access1.insert(date1);
            access2.insert(date2);
            access3.insert(date3);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(3, access1.count());
        assertEquals(date1, access1.selectById(1));
        assertEquals(date2, access1.selectById(2));
        assertEquals(date3, access1.selectById(3));
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        long currentTimestamp = new Date().getTime();
        String date1 = new Date(currentTimestamp).toString();
        String date2 = new Date(currentTimestamp + 1000).toString();
        String date3 = new Date(currentTimestamp + 2000).toString();
        manager.start();
        try {
            access1.insert(date1);
            access2.insert(date2 + "_oversize_string");
            access3.insert(date3);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testMultipleInserts_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        long currentTimestamp = new Date().getTime();
        String date1 = new Date(currentTimestamp).toString();
        String date2 = new Date(currentTimestamp + 1000).toString();
        String date3 = new Date(currentTimestamp + 2000).toString();
        manager.start();
        try {
            access1.insertWithTransaction(date1);
            access2.insertWithTransaction(date2);
            access3.insertWithTransaction(date3);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(3, access1.count());
        assertEquals(date1, access1.selectById(1));
        assertEquals(date2, access1.selectById(2));
        assertEquals(date3, access1.selectById(3));
    }

    @Test
    public void testMultipleInserts_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        long currentTimestamp = new Date().getTime();
        String date1 = new Date(currentTimestamp).toString();
        String date2 = new Date(currentTimestamp + 1000).toString();
        String date3 = new Date(currentTimestamp + 2000).toString();
        manager.start();
        try {
            access1.insertWithTransaction(date1);
            access2.insertWithTransaction(date2 + "_oversize_string");
            access3.insertWithTransaction(date3);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_NoneTransactionalInsert() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access.insert(date);
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(1));
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback_NoneTransactionalInsert() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access.insert(date);
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_NoneTransactionalInsert_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
        thrown.expect(RuntimeException.class);
        access.insert(date + "_oversize_string");
        access.insert(date);
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_TransactionalInsert() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access.insertWithTransaction(date);
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(1));
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback_TransactionalInsert() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access.insertWithTransaction(date);
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_TransactionalInsert_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
        thrown.expect(RuntimeException.class);
        access.insertWithTransaction(date + "_oversize_string");
        access.insertWithTransaction(date);
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_SingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(1));
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback_SingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_SingleInsert_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback_SingleInsert_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access.count());
    }

    @Test
    public void testSingleInsert_LocalTransaction_SingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(1));
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_LocalTransaction_Rollback_SingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insertWithTransaction(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_LocalTransaction_SingleInsert_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_LocalTransaction_Rollback_SingleInsert_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insertWithTransaction(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access.count());
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_SingleInsert_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(2, access.count());
        assertEquals(date, access.selectById(1));
        assertEquals(date, access.selectById(2));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback_SingleInsert_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_SingleInsert_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insertWithTransaction(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(1, access.count());
        assertEquals(date, access.selectById(1));
    }

    @Test
    public void testSingleInsert_NoLocalTransaction_Rollback_SingleInsert_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access = new TestAccess(dataSource, provider);
        manager.register("access", access);

        String date = new Date().toString();
        manager.start();
        try {
            access.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access.insertWithTransaction(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access.count());
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_MultipleInserts_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(6, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Rollback_MultipleInserts_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = (0 + 3); i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_MultipleInserts_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Rollback_MultipleInserts_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testMultipleInserts_LocalTransaction_MultipleInserts_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date);
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(6, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_LocalTransaction_Rollback_MultipleInserts_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = (0 + 3); i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_LocalTransaction_MultipleInserts_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date);
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_LocalTransaction_Rollback_MultipleInserts_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_MultipleInserts_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date);
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(6, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Rollback_MultipleInserts_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date);
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = (0 + 3); i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_MultipleInserts_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Rollback_MultipleInserts_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insert(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        manager.start();
        try {
            access1.insertWithTransaction(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insertWithTransaction(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Mix_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        int count = access1.count();
        assertEquals(3, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Mix_LocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testMultipleInserts_LocalTransaction_Mix_NoLocalTransaction_Rollback() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date);
            access3.insert(date + "_oversize_string");
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        assertEquals(0, access1.count());
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Mix_LocalTransaction_SingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access1.insert(date);
        int count = access1.count();
        assertEquals(4, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Mix_LocalTransaction_Rollback_SingleInsert_NoLocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access1.insert(date);
        int count = access1.count();
        assertEquals(1, count);
        for (int i = (0 + 3); i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Mix_LocalTransaction_SingleInsert_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date);
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access1.insertWithTransaction(date);
        int count = access1.count();
        assertEquals(4, count);
        for (int i = 0; i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    @Test
    public void testMultipleInserts_NoLocalTransaction_Mix_LocalTransaction_Rollback_SingleInsert_LocalTransaction() {
        TransactionManager manager = new TransactionManagerImpl();
        TransactionHelperProvider provider = new TransactionHelperProvider();
        TestAccess access1 = new TestAccess(dataSource, provider);
        TestAccess access2 = new TestAccess(dataSource, provider);
        TestAccess access3 = new TestAccess(dataSource, provider);
        manager.register("access1", access1);
        manager.register("access2", access2);
        manager.register("access3", access3);

        String date = new Date().toString();
        manager.start();
        try {
            access1.insert(date);
            access2.insertWithTransaction(date + "_oversize_string");
            access3.insert(date);
            manager.commit();
        } catch (Exception e) {
            manager.rollback();
        }
        access1.insertWithTransaction(date);
        int count = access1.count();
        assertEquals(1, count);
        for (int i = (0 + 3); i < count; i ++) {
            assertEquals(date, access1.selectById(i + 1));
        }
    }

    private static class TestAccess extends JDBCTransactionalService {

        public TestAccess(DataSource dataSource, TransactionHelperProvider helperProvider) {
            super(dataSource, helperProvider);
        }

        public void insert(String value) {
            Connection conn = getConnection();
            PreparedStatement stmt=null;
            try {
                String sql = String.format("INSERT INTO %s (%s) VALUES (%s);",
                        SCHEMA_NAME + "." + TABLE_NAME,
                        "value",
                        "?");
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, value);

                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                closePreparedStatement(stmt);
                closeConnection(conn);
            }
        }

        public void insertWithTransaction(String value) {
            start();
            Connection conn = getConnection();
            PreparedStatement stmt=null;
            try {
                String sql = String.format("INSERT INTO %s (%s) VALUES (%s);",
                        SCHEMA_NAME + "." + TABLE_NAME,
                        "value",
                        "?");
                stmt = conn.prepareStatement(sql);
                stmt.setString(1, value);
                stmt.executeUpdate();
                commit();
            } catch (SQLException e) {
                rollback();
                throw new RuntimeException(e);
            } finally {
                closePreparedStatement(stmt);
                closeConnection(conn);
            }
        }

        public String selectById(int id) {
            Connection conn = getConnection();
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = String.format("SELECT * FROM %s WHERE id=? LIMIT 1;",
                        SCHEMA_NAME + "." + TABLE_NAME);
                stmt = conn.prepareCall(sql);
                stmt.setInt(1, id);
                rs = stmt.executeQuery();
                if (!rs.next()) {
                    return null;
                } else {
                    return rs.getString("value");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                closeResultSet(rs);
                closePreparedStatement(stmt);
                closeConnection(conn);
            }
        }

        public int count() {
            Connection conn = getConnection();
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = String.format("SELECT count(*) FROM %s;",
                        SCHEMA_NAME + "." + TABLE_NAME);
                stmt = conn.prepareCall(sql);
                rs = stmt.executeQuery();
                rs.next();
                return rs.getInt(1);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                closeResultSet(rs);
                closePreparedStatement(stmt);
                closeConnection(conn);
            }
        }
    }

}
