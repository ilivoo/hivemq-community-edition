package com.hivemq.persistence.deliver;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.publish.PUBLISH;

import java.util.Set;

public class PublishWithSenderDeliver extends PUBLISH {

    private Long deliverId;

    private String sender;

    private Set<String> delivers;

    public PublishWithSenderDeliver(
            @NotNull final PUBLISH publish,
            @NotNull String sender,
            @NotNull Set<String> delivers) {
        super(publish, publish.getPayloadId(), publish.getPersistence());
        this.sender = sender;
        this.delivers = delivers;
    }

    public String getSender() {
        return sender;
    }

    public Long getDeliverId() {
        return deliverId;
    }

    public void setDeliverId(final Long deliverId) {
        this.deliverId = deliverId;
    }

    public Set<String> getDelivers() {
        return delivers;
    }

    public void setDelivers(final Set<String> delivers) {
        this.delivers = delivers;
    }
}
