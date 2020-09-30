package com.hivemq.persistence.deliver;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.AbstractPersistence;
import com.hivemq.persistence.ProducerQueues;
import com.hivemq.persistence.SingleWriterService;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.persistence.util.FutureUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

@LazySingleton
public class DeliverMessagePersistenceImpl extends AbstractPersistence implements DeliverMessagePersistence {

    private static final Logger log = LoggerFactory.getLogger(DeliverMessagePersistenceImpl.class);

    private final @NotNull PublishPayloadPersistence payloadPersistence;
    private final @NotNull ProducerQueues singleWriter;
    private final @NotNull DeliverMessageLocalPersistence localPersistence;
    private final @NotNull int bucketCount;

    private final @NotNull AtomicLong nextDeliverId = new AtomicLong(0);

    @Inject
    DeliverMessagePersistenceImpl(
            final @NotNull PublishPayloadPersistence payloadPersistence,
            final @NotNull SingleWriterService singleWriterService,
            final @NotNull DeliverMessageLocalPersistence localPersistence) {
        this.payloadPersistence = payloadPersistence;
        this.singleWriter = singleWriterService.getDeliverMessageQueue();
        this.localPersistence = localPersistence;
        this.bucketCount = InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get();
    }

    @Override
    public void init() {
        nextDeliverId.set(localPersistence.getMaxId());
    }

    @Override
    public long size() {
        return localPersistence.size();
    }

    @Override
    public @NotNull ListenableFuture<Void> persist(PublishWithSenderDeliver publishWith) {
        try {
            checkNotNull(publishWith, "deliver message must not be null");

            if (publishWith.getDeliverId() == null) {
                publishWith.setDeliverId(nextDeliverId.incrementAndGet());
            }

            if (publishWith.getPayloadId() == null) {
                final long payloadId = payloadPersistence.add(publishWith.getPayload(), 1);
                publishWith.setPayloadId(payloadId);
                publishWith.setPersistence(payloadPersistence);
            }

            return singleWriter.submit(publishWith.getDeliverId().toString(), (bucketIndex, queueBuckets, queueIndex) -> {
                localPersistence.put(publishWith, bucketIndex);
                return null;
            });

        } catch (final Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
    }

    @Override
    public @NotNull ListenableFuture<PublishWithSenderDeliver> get(long deliverId) {
        try {
            return singleWriter.submit(String.valueOf(deliverId), (bucketIndex, queueBuckets, queueIndex) -> {
                final PublishWithSenderDeliver publishWith = localPersistence.get(deliverId, bucketIndex);
                if (publishWith == null) {
                    return null;
                }
                //maybe cache payload again
                //payloadPersistence.incrementReferenceCounterOnBootstrap(publishWith.getPayloadId());
                //final long mayChangePayloadId = payloadPersistence.add(publishWith.getPayload(), 1);
                //publishWith.setPayloadId(mayChangePayloadId);
                //publishWith.setPersistence(payloadPersistence);
                return publishWith;
            });

        } catch (final Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
    }

    @Override
    public @NotNull ListenableFuture<Void> remove(long deliverId) {
        try {
            return singleWriter.submit(String.valueOf(deliverId), (bucketIndex, queueBuckets, queueIndex) -> {
                localPersistence.remove(deliverId, bucketIndex);
                return null;
            });
        } catch (final Throwable throwable) {
            return Futures.immediateFailedFuture(throwable);
        }
    }

    @Override
    public @NotNull ListenableFuture<Void> closeDB() {
        return closeDB(localPersistence, singleWriter);
    }

    @Override
    public @NotNull ListenableFuture<Void> clear() {
        final List<ListenableFuture<Void>> futureList =
                singleWriter.submitToAllQueues((bucketIndex, queueBuckets, queueIndex) -> {
                    for (final Integer bucket : queueBuckets) {
                        localPersistence.clear(bucket);
                    }
                    return null;
                });
        return FutureUtils.voidFutureFromList(ImmutableList.copyOf(futureList));
    }

    public void iterate(@NotNull DeliverMessagePersistence.PublishIteration callback) {
        Iterator<Long> ids = idIterator();
        IterationContextImpl context = new IterationContextImpl();
        while (ids.hasNext()) {
            long deliverId = ids.next();
            ListenableFuture<PublishWithSenderDeliver> publishFuture = get(deliverId);
            PublishWithSenderDeliver publishWith = null;
            try {
                publishWith = publishFuture.get();
            } catch (Exception e) {
                log.error("get deliver message error", e);
            }
            if (publishWith == null) {
                log.warn("deliver message does`nt exist, deliver id {}", deliverId);
                continue;
            }
            callback.iterate(context, publishWith);
            if (context.isAbort()) {
                break;
            }
        }
    }

    @Override
    public Iterator<Long> idIterator() {
        return new AbstractIterator<>() {
            Iterator<Long> bucketIterator;
            int currentBucketIndex;

            @Override
            protected Long computeNext() {
                Long result = compute();
                if (result != null) {
                    return result;
                }
                return endOfData();
            }

            private Long compute() {
                if (bucketIterator != null && bucketIterator.hasNext()) {
                    return bucketIterator.next();
                }
                if (currentBucketIndex < bucketCount) {
                    bucketIterator = localPersistence.getIds(currentBucketIndex).iterator();
                    currentBucketIndex += 1;
                } else {
                    return null;
                }
                return compute();
            }
        };
    }

    private static class IterationContextImpl implements IterationContext {
        private boolean abort;

        public boolean isAbort() {
            return abort;
        }

        public void abortIteration() {
            this.abort = true;
        }
    }
}
