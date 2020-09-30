package com.hivemq.persistence.deliver;

import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.local.xodus.EnvironmentUtil;
import com.hivemq.persistence.local.xodus.XodusLocalPersistence;
import com.hivemq.persistence.local.xodus.bucket.Bucket;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.util.LocalPersistenceFileUtil;
import com.hivemq.util.ThreadPreConditions;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hivemq.persistence.local.xodus.XodusUtils.*;
import static com.hivemq.util.ThreadPreConditions.SINGLE_WRITER_THREAD_PREFIX;

@LazySingleton
public class DeliverMessageXodusLocalPersistence extends XodusLocalPersistence implements DeliverMessageLocalPersistence{

    private static final Logger log = LoggerFactory.getLogger(DeliverMessageXodusLocalPersistence.class);

    public static final String PERSISTENCE_VERSION = "040000";

    private final AtomicLong deliverMessageCounter = new AtomicLong(0);

    private final @NotNull PublishPayloadPersistence payloadPersistence;

    private final @NotNull DeliverMessageXodusSerializer serializer;
    /* we just track increment maxId, not track remove*/
    private AtomicLong maxId = new AtomicLong(0);

    @Inject
    public DeliverMessageXodusLocalPersistence(
            final @NotNull EnvironmentUtil environmentUtil,
            final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
            final @NotNull PersistenceStartup persistenceStartup,
            final @NotNull PublishPayloadPersistence payloadPersistence) {
        super(environmentUtil,
                localPersistenceFileUtil,
                persistenceStartup,
                InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get(),
                InternalConfigurations.DELIVER_MESSAGE_PERSISTENCE_TYPE.get().equals(PersistenceType.FILE));
        this.payloadPersistence = payloadPersistence;
        this.serializer = new DeliverMessageXodusSerializer(payloadPersistence);
    }

    @PostConstruct
    protected void postConstruct() {
        super.postConstruct();
    }

    @Override
    public long size() {
        return deliverMessageCounter.get();
    }

    @Override
    public void put(PublishWithSenderDeliver publishWith, int bucketIndex) {
        final Long deliverId = publishWith.getDeliverId();
        checkNotNull(publishWith, "Deliver message must not be null");
        checkNotNull(deliverId, "Deliver message id must not be null");
        checkNotNull(publishWith.getPayloadId(), "Deliver message payload id must not be null");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Bucket bucket = buckets[bucketIndex];

        bucket.getEnvironment().executeInExclusiveTransaction(txn -> {
            ByteIterable key = bytesToByteIterable(serializer.serializeKey(deliverId));
            ByteIterable value = bytesToByteIterable(serializer.serializeValue(publishWith));
            try (final Cursor cursor = bucket.getStore().openCursor(txn)) {
                final ByteIterable byteIterable = cursor.getSearchKey(key);
                if (byteIterable != null) {
                    log.trace("Replacing deliver message with deliver id {}", deliverId);
                    bucket.getStore().put(txn, key, value);
                    //payload will never change, so no need decrement reference
                    //payloadPersistence.decrementReferenceCounter(payloadId);
                } else {
                    log.trace("Creating new deliver message with deliver id {}", deliverId);
                    bucket.getStore().put(txn, key, value);
                    deliverMessageCounter.incrementAndGet();
                }
            }
        });
        maxId.getAndUpdate(prev -> {
            return deliverId > prev ? deliverId : prev;
        });
    }

    @Override
    public PublishWithSenderDeliver get(long deliverId, int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Bucket bucket = buckets[bucketIndex];

        return bucket.getEnvironment().computeInReadonlyTransaction(txn -> {
            ByteIterable key = bytesToByteIterable(serializer.serializeKey(deliverId));
            ByteIterable byteIterable = bucket.getStore().get(txn, key);
            if (byteIterable == null) {
                return null;
            }
            PublishWithSenderDeliver publishWith = serializer.deserializeValue(byteIterableToBytes(byteIterable));

            final byte[] payload = payloadPersistence.getPayloadOrNull(publishWith.getPayloadId());
            if (payload == null) {
                log.warn("deliver publish [{}] is not null, but payload is null, delete deliver publish", deliverId);
                bucket.getStore().delete(txn, key);
                return null;
            }
            publishWith.setDeliverId(deliverId);
            publishWith.setPayload(payload);
            publishWith.setPersistence(payloadPersistence);
            return publishWith;
        });
    }

    @Override
    public void remove(long deliverId, int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        final Bucket bucket = buckets[bucketIndex];

        bucket.getEnvironment().executeInExclusiveTransaction(txn -> {
            final ByteIterable key = bytesToByteIterable(serializer.serializeKey(deliverId));
            final ByteIterable byteIterable = bucket.getStore().get(txn, key);
            if (byteIterable == null) {
                log.trace("Removing deliver message with payload id {} (no message was stored previously)", deliverId);
                return;
            }
            PublishWithSenderDeliver publishWith = serializer.deserializeValue(byteIterableToBytes(byteIterable));
            log.trace("Removing deliver message with deliver id {}", deliverId);
            bucket.getStore().delete(txn, key);
            payloadPersistence.decrementReferenceCounter(publishWith.getPayloadId());
            deliverMessageCounter.decrementAndGet();
        });
    }

    @Override
    public void clear(int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Bucket bucket = buckets[bucketIndex];

        bucket.getEnvironment().executeInExclusiveTransaction(txn -> {
            final Cursor cursor = bucket.getStore().openCursor(txn);
            while (cursor.getNext()) {
                PublishWithSenderDeliver publishWith = serializer.deserializeValue(byteIterableToBytes(cursor.getValue()));
                cursor.deleteCurrent();
                payloadPersistence.decrementReferenceCounter(publishWith.getPayloadId());
                deliverMessageCounter.decrementAndGet();
            }
        });
    }

    @Override
    public List<Long> getIds(final int bucketIndex) {
        List<Long> result = new ArrayList<>();
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final Bucket bucket = buckets[bucketIndex];

        bucket.getEnvironment().executeInExclusiveTransaction(txn -> {
            final Cursor cursor = bucket.getStore().openCursor(txn);
            while (cursor.getNext()) {
                long deliverId = serializer.deserializeKey(byteIterableToBytes(cursor.getKey()));
                result.add(deliverId);
            }
        });
        return result;
    }

    @Override
    public void iterate(@NotNull ItemCallback callBack) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        for (final Bucket bucket : buckets) {
            bucket.getEnvironment().executeInReadonlyTransaction(txn -> {
                try (Cursor cursor = bucket.getStore().openCursor(txn)) {
                    while (cursor.getNext()) {
                        long deliverId = serializer.deserializeKey(byteIterableToBytes(cursor.getKey()));
                        PublishWithSenderDeliver publishWith = serializer.deserializeValue(byteIterableToBytes(cursor.getValue()));
                        final byte[] payload = payloadPersistence.getPayloadOrNull(publishWith.getPayloadId());
                        if (payload == null) {
                            log.warn("deliver publish [{}] is not null, but payload is null, delete deliver publish", deliverId);
                            bucket.getStore().delete(txn, cursor.getKey());
                            continue;
                        }
                        publishWith.setDeliverId(deliverId);
                        publishWith.setPayload(payload);
                        publishWith.setPersistence(payloadPersistence);
                        callBack.onItem(publishWith);
                    }
                }
            });
        }
    }

    @Override
    protected @NotNull String getName() {
        return PERSISTENCE_NAME;
    }

    @Override
    protected @NotNull String getVersion() {
        return PERSISTENCE_VERSION;
    }

    @Override
    protected @NotNull StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES;
    }

    @Override
    protected @NotNull Logger getLogger() {
        return log;
    }

    @Override
    public void init() {
        try {
            final AtomicLong prevMax = new AtomicLong(0);
            for (int i = 0; i < buckets.length; i++) {
                final Bucket bucket = buckets[i];
                bucket.getEnvironment().executeInReadonlyTransaction(txn -> {
                    try (final Cursor cursor = bucket.getStore().openCursor(txn)) {

                        while (cursor.getNext()) {
                            long deliverId = serializer.deserializeKey(byteIterableToBytes(cursor.getKey()));
                            prevMax.getAndUpdate(prev -> {
                                return deliverId > prev ? deliverId : prev;
                            });
                            PublishWithSenderDeliver publishWith = serializer.deserializeValue(byteIterableToBytes(cursor.getValue()));
                            payloadPersistence.incrementReferenceCounterOnBootstrap(publishWith.getPayloadId());
                            deliverMessageCounter.incrementAndGet();
                        }
                    }
                });
            }
            maxId.set(prevMax.get());
        } catch (final ExodusException e) {
            log.error("An error occurred while preparing the Deliver Message persistence.");
            log.debug("Original Exception:", e);
            throw new UnrecoverableException(false);
        }
    }

    @Override
    public long getMaxId() {
        return maxId.get();
    }
}
