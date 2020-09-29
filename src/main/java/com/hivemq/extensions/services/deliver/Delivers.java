package com.hivemq.extensions.services.deliver;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.services.deliver.PublishDeliver;

import java.util.Map;

public interface Delivers {

    void addPublishDeliver(@NotNull PublishDeliver publishDeliver);

    @NotNull Map<@NotNull String, @NotNull PublishDeliver> getPublishDeliverMap();
}
