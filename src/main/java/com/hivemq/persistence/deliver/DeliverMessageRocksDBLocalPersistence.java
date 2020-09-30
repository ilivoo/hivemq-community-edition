package com.hivemq.persistence.deliver;

import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.exceptions.UnrecoverableException;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.PersistenceStartup;
import com.hivemq.persistence.local.rocksdb.RocksDBLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.util.LocalPersistenceFileUtil;
import com.hivemq.util.ThreadPreConditions;
import jetbrains.exodus.ExodusException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hivemq.util.ThreadPreConditions.SINGLE_WRITER_THREAD_PREFIX;

@LazySingleton
public class DeliverMessageRocksDBLocalPersistence extends RocksDBLocalPersistence
        implements DeliverMessageLocalPersistence {

    private static final Logger log = LoggerFactory.getLogger(DeliverMessageRocksDBLocalPersistence.class);

    public static final String PERSISTENCE_VERSION = "040000_R";

    private final AtomicLong deliverMessageCounter = new AtomicLong(0);

    private final @NotNull PublishPayloadPersistence payloadPersistence;

    private final @NotNull DeliverMessageXodusSerializer serializer;

    private AtomicLong maxId = new AtomicLong(0);

    @Inject
    DeliverMessageRocksDBLocalPersistence(
            final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
            final @NotNull PersistenceStartup persistenceStartup,
            final @NotNull PublishPayloadPersistence payloadPersistence) {
        super(
                localPersistenceFileUtil,
                persistenceStartup,
                InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get(),
                InternalConfigurations.DELIVER_MESSAGE_MEMTABLE_SIZE_PORTION,
                InternalConfigurations.DELIVER_MESSAGE_BLOCK_CACHE_SIZE_PORTION,
                InternalConfigurations.DELIVER_MESSAGE_BLOCK_SIZE,
                InternalConfigurations.DELIVER_MESSAGE_PERSISTENCE_TYPE.get() == PersistenceType.FILE_NATIVE);

        this.payloadPersistence = payloadPersistence;
        this.serializer = new DeliverMessageXodusSerializer(payloadPersistence);
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
    protected @NotNull Options getOptions() {
        return new Options().setCreateIfMissing(true).setStatistics(new Statistics());
    }

    @Override
    protected @NotNull Logger getLogger() {
        return log;
    }

    @PostConstruct
    protected void postConstruct() {
        super.postConstruct();
    }

    @Override
    public void init() {
        try {
            long max = 0;
            for (int i = 0; i < buckets.length; i++) {
                final RocksDB bucket = buckets[i];
                try (final RocksIterator iterator = bucket.newIterator()) {
                    iterator.seekToFirst();
                    while (iterator.isValid()) {
                        long deliverId = serializer.deserializeKey(iterator.key());
                        if (deliverId > max) {
                            max = deliverId;
                        }
                        final PublishWithSenderDeliver publishWith = serializer.deserializeValue(iterator.value());
                        payloadPersistence.incrementReferenceCounterOnBootstrap(publishWith.getPayloadId());
                        deliverMessageCounter.incrementAndGet();
                        iterator.next();
                    }
                }
            }
            maxId.set(max);
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

    @Override
    public long size() {
        return deliverMessageCounter.get();
    }

    @Override
    public void put(PublishWithSenderDeliver publishWith, int bucketIndex) {
        final Long deliverId = publishWith.getDeliverId();
        checkNotNull(publishWith, "Deliver message must not be null");
        checkNotNull(deliverId, "Deliver message payload id must not be null");
        checkNotNull(publishWith.getPayloadId(), "Deliver message payload id must not be null");
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RocksDB bucket = buckets[bucketIndex];

        try {
            final byte[] serializedKey = serializer.serializeKey(deliverId);
            final byte[] serializedValue = serializer.serializeValue(publishWith);

            final byte[] valueAsBytes = bucket.get(serializedKey);
            if (valueAsBytes != null) {
                log.trace("Replacing deliver message with deliver id {}", deliverId);
                bucket.put(serializedKey, serializedValue);
                //payload will never change, so no need decrement reference
                //payloadPersistence.decrementReferenceCounter(payloadId);
            } else {
                log.trace("Creating new deliver message with deliver id {}", deliverId);
                bucket.put(serializedKey, serializedValue);
                deliverMessageCounter.incrementAndGet();
            }

        } catch (final Exception e) {
            log.error("An error occurred while persisting a deliver message.");
            log.debug("Original Exception:", e);
        }
        maxId.getAndUpdate(prev -> {
            return deliverId > prev ? deliverId : prev;
        });
    }

    @Override
    public PublishWithSenderDeliver get(long deliverId, int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RocksDB bucket = buckets[bucketIndex];
        try {
            byte[] key = serializer.serializeKey(deliverId);
            final byte[] valueBytes = bucket.get(key);
            if (valueBytes == null) {
                return null;
            }

            final PublishWithSenderDeliver publishWith = serializer.deserializeValue(valueBytes);
            final byte[] payload = payloadPersistence.getPayloadOrNull(publishWith.getPayloadId());
            if (payload == null) {
                log.warn("deliver publish [{}] is not null, but payload is null, delete deliver publish", deliverId);
                bucket.delete(key);
                return null;
            }

            publishWith.setDeliverId(deliverId);
            publishWith.setPayload(payload);
            publishWith.setPersistence(payloadPersistence);
            return publishWith;
        } catch (Exception e) {
            log.error("An error occurred while get a deliver message.");
            log.debug("Original Exception:", e);
        }
        return null;
    }

    @Override
    public void remove(long deliverId, int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        final RocksDB bucket = buckets[bucketIndex];

        try {
            final byte[] key = serializer.serializeKey(deliverId);
            final byte[] removed = bucket.get(key);
            if (removed == null) {
                log.trace("Removing deliver message with deliver id {} (no message was stored previously)", deliverId);
                return;
            }
            log.trace("Removing deliver message with deliver id {}", deliverId);
            bucket.delete(key);
            final PublishWithSenderDeliver publishWith = serializer.deserializeValue(removed);
            payloadPersistence.decrementReferenceCounter(publishWith.getPayloadId());
            deliverMessageCounter.decrementAndGet();
        } catch (final Exception e) {
            log.error("An error occurred while removing a deliver message.");
            log.debug("Original Exception:", e);
        }
    }

    @Override
    public void clear(int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        final RocksDB bucket = buckets[bucketIndex];
        try (final WriteBatch writeBatch = new WriteBatch(); final WriteOptions options = new WriteOptions();
             final RocksIterator iterator = bucket.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                PublishWithSenderDeliver publishWith = serializer.deserializeValue(iterator.value());
                payloadPersistence.decrementReferenceCounter(publishWith.getPayloadId());
                deliverMessageCounter.decrementAndGet();
                writeBatch.delete(iterator.key());
                iterator.next();
            }
            bucket.write(options, writeBatch);
        } catch (final Exception e) {
            log.error("An error occurred while clearing the deliver message persistence.");
            log.debug("Original Exception:", e);
        }
    }

    @Override
    public List<Long> getIds(final int bucketIndex) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);
        List<Long> result = new ArrayList<>();
        final RocksDB bucket = buckets[bucketIndex];
        try (final WriteBatch writeBatch = new WriteBatch(); final WriteOptions options = new WriteOptions();
             final RocksIterator iterator = bucket.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                long deliverId = serializer.deserializeKey(iterator.key());
                result.add(deliverId);
                iterator.next();
            }
            bucket.write(options, writeBatch);
        } catch (final Exception e) {
            log.error("An error occurred while clearing the deliver message persistence.");
            log.debug("Original Exception:", e);
        }
        return result;
    }

    @Override
    public void iterate(@NotNull ItemCallback callBack) {
        ThreadPreConditions.startsWith(SINGLE_WRITER_THREAD_PREFIX);

        for (final RocksDB bucket : buckets) {
            Map<Long, byte[]> deleteMap = new HashMap<>();
            try (final RocksIterator iterator = bucket.newIterator()) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    long deliverId = serializer.deserializeKey(iterator.key());
                    PublishWithSenderDeliver publishWith = serializer.deserializeValue(iterator.value());
                    final byte[] payload = payloadPersistence.getPayloadOrNull(publishWith.getPayloadId());
                    if (payload == null) {
                        deleteMap.put(deliverId, iterator.key());
                        continue;
                    }
                    publishWith.setDeliverId(deliverId);
                    publishWith.setPayload(payload);
                    publishWith.setPersistence(payloadPersistence);
                    callBack.onItem(publishWith);
                    iterator.next();
                }
            } finally {
                for (Map.Entry<Long, byte[]> entry : deleteMap.entrySet()) {
                    try {
                        log.warn("deliver publish [{}] is not null, but payload is null, delete deliver publish",
                                entry.getKey());
                        bucket.delete(entry.getValue());
                    } catch (Exception e) {
                        log.error("An error occurred while delete the deliver message persistence.");
                        log.debug("Original Exception:", e);
                        break;
                    }
                }
            }
        }
    }
}
