package com.hivemq.persistence.deliver;

import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;

import javax.inject.Inject;
import javax.inject.Provider;

public class DeliverMessagePersistenceProvider implements Provider<DeliverMessagePersistence> {

    private final Provider<DeliverMessagePersistenceImpl> provider;

    @Inject
    public DeliverMessagePersistenceProvider(final Provider<DeliverMessagePersistenceImpl> provider) {
        this.provider = provider;
    }

    @Override
    @LazySingleton
    public DeliverMessagePersistence get() {
        return provider.get();
    }
}
