package com.codingzero.utilities.transaction.impl;

import com.codingzero.utilities.transaction.TransactionContext;
import com.codingzero.utilities.transaction.TransactionManager;
import com.codingzero.utilities.transaction.TransactionalService;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class TransactionManagerImpl implements TransactionManager {

    private Map<String, TransactionalService> services;
    private TransactionContext context;

    public TransactionManagerImpl() {
        this.services = new LinkedHashMap<>();
        this.context = new TransactionContextImpl();
    }

    @Override
    public void register(String name, TransactionalService service) {
        checkForInvalidServiceNameFormat(name);
        services.put(name.toLowerCase(), service);
        service.onRegister(name, context);
    }

    private void checkForInvalidServiceNameFormat(String name) {
        if (null == name || name.trim().length() == 0) {
            throw new IllegalArgumentException("Service name cannot be null value or empty string.");
        }
        if (name.length() <= 2 || name.length() > 99) {
            throw new IllegalArgumentException(
                    "Service name need to be greater than 3 characters and less than 100 characters. ");
        }
    }

    @Override
    public TransactionalService deregister(String name) {
        return services.remove(name);
    }

    @Override
    public void startTransaction() {
        for (TransactionalService service: services.values()) {
            service.onStartTransaction(context);
        }
    }

    @Override
    public void commit() {
        for (TransactionalService service: services.values()) {
            service.onCommitTransaction(context);
        }
        this.context = null;
    }

    @Override
    public void rollback() {
        for (TransactionalService service: services.values()) {
            service.onRollbackTransaction(context);
        }
        this.context = null;
    }

    private static class TransactionContextImpl implements TransactionContext {

        private Map<String, Object> properties;

        public TransactionContextImpl() {
            this.properties = new HashMap<>();
        }

        @Override
        public void setProperty(String name, Object property) {
            properties.put(name, property);
        }

        @Override
        public Object getProperty(String name) {
            return properties.get(name);
        }

        @Override
        public Object removeProperty(String name) {
            return properties.remove(name);
        }
    }
}