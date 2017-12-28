The goal of this utility is to let you can easily implement across multiple resources transaction management for your software application.

Comparing to JTA (Java transaction API), this utility is more lightweight and less enterprisey.

This utility is designed with observer pattern, and the core is only about two interfaces: *TransactionManager* and *TransactionalService*.

##### TransactionManager
TransactionManager is a central place for all resources which implements *TransactionalService* interface, also, the only interface for client to start, commit and rollback a transaction.

##### TransactionalService
If you want a resource have the ability to perform operations according to the different states of a transaction, then, just implement this interface and register into a *TransactionManager*.

### Example

```java
public class StudentDAOImpl extends JDBCTransactionalService implements StudentDAO {
    
    public void insert(Student student) {
        Connection conn = getConnection();
        PreparedStatement stmt = null;
        try {
            //insert student
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closePreparedStatement(stmt);
            closeConnection(conn);
        }
    }    
    
}
```
```java
public class CacheImpl implements TransactionalService {
    
    private Map<String, Object> unCommittedCache; 
    
    public void set(String key, Object value) {
        if (null == unCommittedCache) {
            //cache value with the given key.
        } else {
            unCommittedCache.put(key, value); //    
        }
    }
    
    public void onRegister(String name, TransactionContext context) {
        
    }
    
    public void onStartTransaction(TransactionContext context) {
        unCommittedCache = new HashMap<>();
    }
    
    public void onCommitTransaction(TransactionContext context) {
        for (Map.Entry<String, Object> entry: unCommittedCache.entrySet()) {
            //cache entry.
        }       
    }
    
    public void onRollbackTransaction(TransactionContext context) {        
        unCommittedCache = null;
    }
    
}
```
```java
public class App {
            
    public void registerStudent(String studentName) {
        transactionManager.startTransaction();
        try {
            studentDAO.insert(newStudent);
            cache.set(newStudent.getId(), newStudent);
            transactionManager.commit();
        } catch (RuntimeException e) {
            transactionManager.rollback();
            throw e;
        }
        
    }
    
}
```     

