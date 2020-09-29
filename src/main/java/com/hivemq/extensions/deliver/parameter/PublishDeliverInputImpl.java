package com.hivemq.extensions.deliver.parameter;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.client.parameter.ClientInformation;
import com.hivemq.extension.sdk.api.services.deliver.parameter.PublishDeliverInput;
import com.hivemq.extension.sdk.api.services.publish.Publish;
import com.hivemq.extensions.executor.task.PluginTaskInput;

import java.util.function.Supplier;

public class PublishDeliverInputImpl implements PublishDeliverInput, PluginTaskInput,
        Supplier<PublishDeliverInputImpl> {

    private ClientInformation clientInformation;

    private Publish publish;

    public PublishDeliverInputImpl(ClientInformation clientInformation, Publish publish) {
        this.clientInformation = clientInformation;
        this.publish = publish;
    }

    @Override
    public @NotNull ClientInformation getClientInformation() {
        return clientInformation;
    }

    @Override
    public @NotNull Publish getPublish() {
        return publish;
    }

    @Override
    public PublishDeliverInputImpl get() {
        return this;
    }
}
