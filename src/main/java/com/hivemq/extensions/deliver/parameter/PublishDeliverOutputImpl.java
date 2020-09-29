package com.hivemq.extensions.deliver.parameter;

import com.google.common.util.concurrent.SettableFuture;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.pingreq.parameter.PingReqInboundOutput;
import com.hivemq.extension.sdk.api.services.deliver.parameter.PublishDeliverOutput;
import com.hivemq.extensions.executor.PluginOutPutAsyncer;
import com.hivemq.extensions.executor.task.AbstractAsyncOutput;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;

public class PublishDeliverOutputImpl extends AbstractAsyncOutput<PingReqInboundOutput>
        implements PublishDeliverOutput, Supplier<PublishDeliverOutputImpl> {

    private Set<String> completeDelivers;

    private final @NotNull SettableFuture<Boolean> postFuture;

    public PublishDeliverOutputImpl(final @NotNull PluginOutPutAsyncer asyncer) {
        super(asyncer);
        this.completeDelivers = new CopyOnWriteArraySet<>();
        this.postFuture = SettableFuture.create();
    }

    @Override
    public @NotNull PublishDeliverOutputImpl get() {
        return this;
    }

    public void addComplete(String deliver) {
        this.completeDelivers.add(deliver);
    }

    public Set<String> getCompleteDelivers() {
        return this.completeDelivers;
    }

    public SettableFuture<Boolean> getPostFuture() {
        return postFuture;
    }
}
