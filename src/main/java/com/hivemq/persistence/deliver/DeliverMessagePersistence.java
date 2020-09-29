package com.hivemq.persistence.deliver;

import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.extension.sdk.api.annotations.NotNull;

import java.util.Iterator;

public interface DeliverMessagePersistence {
    long size();

    @NotNull
    ListenableFuture<Void> persist(PublishWithSenderDeliver publishWith);

    @NotNull
    ListenableFuture<PublishWithSenderDeliver> get(long payloadId);

    @NotNull
    ListenableFuture<Void> remove(long payloadId);

    @NotNull
    ListenableFuture<Void> clear();

    @NotNull
    ListenableFuture<Void> closeDB();

    Iterator<Long> idIterator();

    void iterate(@NotNull PublishIteration callback);

    interface PublishIteration {

        void iterate(@NotNull IterationContext context, @NotNull PublishWithSenderDeliver publishWith);
    }

    interface IterationContext {

        void abortIteration();
    }
}
