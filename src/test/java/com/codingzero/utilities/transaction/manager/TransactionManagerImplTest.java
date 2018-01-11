package com.codingzero.utilities.transaction.manager;

import com.codingzero.utilities.transaction.TransactionContext;
import com.codingzero.utilities.transaction.TransactionalService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransactionManagerImplTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private TransactionManagerImpl manager;

    @Before
    public void setUp() {
        manager = new TransactionManagerImpl();
    }

    @Test
    public void testRegister() {
        String serviceName = "Service";
        TransactionalService service = mock(TransactionalService.class);
        manager.register(serviceName, service);
        verify(service, times(1)).onRegister(eq(serviceName), any(TransactionContext.class));
    }

    @Test
    public void testRegister_InvalidName_NullValue() {
        TransactionalService service = mock(TransactionalService.class);
        thrown.expect(IllegalArgumentException.class);
        manager.register(null, service);
    }

    @Test
    public void testRegister_InvalidName_EmptyValue() {
        TransactionalService service = mock(TransactionalService.class);
        thrown.expect(IllegalArgumentException.class);
        manager.register(" ", service);
    }

    @Test
    public void testRegister_InvalidName_TooShort() {
        TransactionalService service = mock(TransactionalService.class);
        thrown.expect(IllegalArgumentException.class);
        manager.register("aa", service);
    }

    @Test
    public void testRegister_InvalidName_TooLong() {
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < 100; i ++) {
            name.append("a");
        }
        TransactionalService service = mock(TransactionalService.class);
        thrown.expect(IllegalArgumentException.class);
        manager.register(name.toString(), service);
    }

    @Test
    public void testDeregister() {
        String serviceName = "Service";
        TransactionalService service = mock(TransactionalService.class);
        manager.register(serviceName, service);
        TransactionalService removedService = manager.deregister(serviceName);
        assertEquals(service, removedService);
    }

    @Test
    public void testDeregister_NotExisting() {
        String serviceName = "Service";
        TransactionalService removedService = manager.deregister(serviceName);
        assertEquals(null, removedService);
    }

    @Test
    public void testStart() {
        TransactionalService service = mock(TransactionalService.class);
        manager.register("Service", service);
        manager.start();
        verify(service, times(1)).onStartTransaction(any(TransactionContext.class));
    }

    @Test
    public void testCommit() {
        TransactionalService service = mock(TransactionalService.class);
        manager.register("Service", service);
        manager.commit();
        verify(service, times(1)).onCommitTransaction(any(TransactionContext.class));
    }

    @Test
    public void testRollback() {
        TransactionalService service = mock(TransactionalService.class);
        manager.register("Service", service);
        manager.rollback();
        verify(service, times(1)).onRollbackTransaction(any(TransactionContext.class));
    }
}
