package com.hivemq.persistence.deliver;

import com.google.common.collect.Lists;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.util.ThreadPreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hivemq.util.ThreadPreConditions.SINGLE_WRITER_THREAD_PREFIX;

@Singleton
public class DeliverMessageMemoryLocalPersistence implements DeliverMessageLocalPersistence {

    private static final Logger log = LoggerFactory.getLogger(DeliverMessageMemoryLocalPersistence.class);

    final private @NotNull Map<Long, PublishWithSenderDeliver>[] buckets;
    private final @NotNull PublishPayloadPersistence payloadPersistence;

    private final int bucketCount;

    private final AtomicLong maxId = new AtomicLong(0);

    @Override
    public long getMaxId() {
        return maxId.get();
    }

    @Inject
    public DeliverMessageMemoryLocalPersistence(@NotNull final PublishPayloadPersistence payloadPersistence) {
        this.payloadPersistence = payloadPersistence;
        bucketCount = InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get();
        buckets = new HashMap[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new ConcurrentHashMap<>();
        }
    }

    @Override
    public long size() {
        int sum = 0;
        for (final Map<Long, PublishWithSenderDeliver> bucket : buckets) {
            sum += bucket.size();
        }
        return sum;
    }

    @Override
    public void put(PublishWithSenderDeliver publishWith, int bucketIndex) {
        final Long deliverId = publishWith.getDeliverId();
        checkNotNull(publishWith, "Deliver message must not be null");
        checkNotNull(deliverId, "Deliver message id must not be null");
        checkNotNull(publishWith.getPayloadId(), "Deliver message payload id must not be null");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Map<Long, PublishWithSenderDeliver> bucket = buckets[bucketIndex];
        bucket.put(deliverId, publishWith);
        maxId.getAndUpdate(prev -> {
            return deliverId > prev ? deliverId : prev;
        });
    }

    @Override
    public PublishWithSenderDeliver get(long deliverId, int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Map<Long, PublishWithSenderDeliver> bucket = buckets[bucketIndex];
        final PublishWithSenderDeliver publishWith = bucket.get(deliverId);
        if (publishWith == null) {
            return null;
        }
        final byte[] payload = payloadPersistence.getPayloadOrNull(publishWith.getPayloadId());
        if (payload == null) {
            log.warn("Payload with ID '{}' for deliver messages not found.", publishWith.getPayloadId());
            bucket.remove(deliverId);
            return null;
        }
        publishWith.setPayload(payload);
        publishWith.setPersistence(payloadPersistence);
        return publishWith;
    }

    @Override
    public void remove(long deliverId, int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Map<Long, PublishWithSenderDeliver> bucket = buckets[bucketIndex];
        final PublishWithSenderDeliver publishWith = bucket.remove(deliverId);
        if (publishWith != null) {
            payloadPersistence.decrementReferenceCounter(publishWith.getPayloadId());
        }
    }

    @Override
    public void clear(int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        final Map<Long, PublishWithSenderDeliver> bucket = buckets[bucketIndex];
        for (PublishWithSenderDeliver publishWith : bucket.values()) {
            payloadPersistence.decrementReferenceCounter(publishWith.getPayloadId());
        }
        bucket.clear();
    }

    @Override
    public List<Long> getIds(final int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        final Map<Long, PublishWithSenderDeliver> bucket = buckets[bucketIndex];
        return Lists.newArrayList(bucket.keySet());
    }

    @Override
    public void iterate(@NotNull ItemCallback callBack) {
        for (int i = 0; i < buckets.length; i++) {
            Map<Long, PublishWithSenderDeliver> bucket = buckets[i];
            for (Long deliverId : bucket.keySet()) {
                PublishWithSenderDeliver publishWith = get(deliverId, i);
                callBack.onItem(publishWith);
            }
        }
    }

    @Override
    public void closeDB(final int bucketIndex) {
        //do nothing
    }
}
