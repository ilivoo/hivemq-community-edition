package com.hivemq.extensions.services.deliver;

import com.google.common.base.Preconditions;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.services.deliver.DeliverRegistry;
import com.hivemq.extension.sdk.api.services.deliver.PublishDeliver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DeliverRegistryImpl implements DeliverRegistry {

    @NotNull
    private final Delivers delivers;

    @Inject
    public DeliverRegistryImpl(@NotNull final Delivers delivers) {
        this.delivers = delivers;
    }

    @Override
    public void setPublishDeliver(@NotNull final PublishDeliver deliver) {
        Preconditions.checkNotNull(deliver, "publish delivers must never be null");
        delivers.addPublishDeliver(deliver);
    }
}
