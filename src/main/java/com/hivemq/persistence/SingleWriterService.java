/*
 * Copyright 2019-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.persistence;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.local.xodus.bucket.BucketUtils;
import com.hivemq.util.Exceptions;
import com.hivemq.util.ThreadFactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.SplittableRandom;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hivemq.configuration.service.InternalConfigurations.SINGLE_WRITER_CHECK_SCHEDULE;

/**
 * @author Lukas Brandl
 */
@LazySingleton
public class SingleWriterService {

    private static final Logger log = LoggerFactory.getLogger(SingleWriterService.class);

    private static final int AMOUNT_OF_PRODUCERS = 6;
    private static final int RETAINED_MESSAGE_QUEUE_INDEX = 0;
    private static final int CLIENT_SESSION_QUEUE_INDEX = 1;
    private static final int SUBSCRIPTION_QUEUE_INDEX = 2;
    private static final int QUEUED_MESSAGES_QUEUE_INDEX = 3;
    private static final int ATTRIBUTE_STORE_QUEUE_INDEX = 4;
    private static final int DELIVER_MESSAGE_QUEUE_INDEX = 5;

    private final int persistenceBucketCount;
    private final int threadPoolSize;
    private final int creditsPerExecution;
    private final long shutdownGracePeriod;

    private final @NotNull AtomicBoolean postConstruct = new AtomicBoolean(true);
    private final @NotNull AtomicLong nonemptyQueueCounter = new AtomicLong(0);
    private final @NotNull AtomicInteger runningThreadsCount = new AtomicInteger(0);
    private final @NotNull AtomicLong globalTaskCount = new AtomicLong(0);

    private final @NotNull ProducerQueues @NotNull [] producers = new ProducerQueues[AMOUNT_OF_PRODUCERS];

    @VisibleForTesting
    @NotNull ExecutorService singleWriterExecutor;
    @VisibleForTesting
    public final @NotNull ExecutorService[] callbackExecutors;
    @VisibleForTesting
    final @NotNull ScheduledExecutorService checkScheduler;

    private final int amountOfQueues;

    @Inject
    public SingleWriterService() {

        persistenceBucketCount = InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get();
        threadPoolSize = InternalConfigurations.SINGLE_WRITER_THREAD_POOL_SIZE.get();
        creditsPerExecution = InternalConfigurations.SINGLE_WRITER_CREDITS_PER_EXECUTION.get();
        shutdownGracePeriod = InternalConfigurations.PERSISTENCE_SHUTDOWN_GRACE_PERIOD.get();

        final ThreadFactory threadFactory = ThreadFactoryUtil.create("single-writer-%d");
        singleWriterExecutor = Executors.newFixedThreadPool(threadPoolSize, threadFactory);

        amountOfQueues = validAmountOfQueues(threadPoolSize, persistenceBucketCount);

        for (int i = 0; i < producers.length; i++) {
            producers[i] = new ProducerQueues(this, amountOfQueues);
        }

        callbackExecutors = new ExecutorService[amountOfQueues];
        for (int i = 0; i < amountOfQueues; i++) {
            final ThreadFactory callbackThreadFactory = ThreadFactoryUtil.create("single-writer-callback-" + i);
            final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor(callbackThreadFactory);
            callbackExecutors[i] = executorService;
        }


        final ThreadFactory checkThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("single-writer-scheduled-check-%d").build();
        checkScheduler = Executors.newSingleThreadScheduledExecutor(checkThreadFactory);
    }

    @PostConstruct
    public void postConstruct() {

        if (!postConstruct.getAndSet(false)) {
            return;
        }

        // Periodically check if there are pending tasks in the queues
        checkScheduler.scheduleAtFixedRate(() -> {
            try {

                if (runningThreadsCount.getAndIncrement() == 0 && !singleWriterExecutor.isShutdown()) {
                    singleWriterExecutor.submit(
                            new SingleWriterTask(nonemptyQueueCounter, globalTaskCount, runningThreadsCount,
                                    producers));
                } else {
                    runningThreadsCount.decrementAndGet();
                }
            } catch (final Exception e) {
                log.error("Exception in single writer check task ", e);
            }
        }, SINGLE_WRITER_CHECK_SCHEDULE.get(), SINGLE_WRITER_CHECK_SCHEDULE.get(), TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    int validAmountOfQueues(final int processorCount, final int bucketCount) {
        for (int i = processorCount; i < bucketCount; i++) {
            if (bucketCount % i == 0) {
                return i;
            }
        }
        return persistenceBucketCount;
    }

    public void incrementNonemptyQueueCounter() {
        nonemptyQueueCounter.incrementAndGet();

        if (runningThreadsCount.getAndIncrement() < threadPoolSize) {
            singleWriterExecutor.submit(
                    new SingleWriterTask(nonemptyQueueCounter, globalTaskCount, runningThreadsCount, producers));
        } else {
            runningThreadsCount.decrementAndGet();
        }
    }

    /**
     * @param key associated with the task
     * @return an executor that will is single threaded and guarantied to be the same for equal keys
     */
    @NotNull
    public ExecutorService callbackExecutor(@NotNull final String key) {
        final int bucketsPerQueue = persistenceBucketCount / amountOfQueues;
        final int bucketIndex = BucketUtils.getBucket(key, persistenceBucketCount);
        final int queueIndex = bucketIndex / bucketsPerQueue;
        return callbackExecutors[queueIndex];
    }

    public void decrementNonemptyQueueCounter() {
        nonemptyQueueCounter.decrementAndGet();
    }

    public ProducerQueues getRetainedMessageQueue() {
        return producers[RETAINED_MESSAGE_QUEUE_INDEX];
    }

    public ProducerQueues getDeliverMessageQueue() {
        return producers[DELIVER_MESSAGE_QUEUE_INDEX];
    }

    public ProducerQueues getClientSessionQueue() {
        return producers[CLIENT_SESSION_QUEUE_INDEX];
    }

    public ProducerQueues getSubscriptionQueue() {
        return producers[SUBSCRIPTION_QUEUE_INDEX];
    }

    public ProducerQueues getQueuedMessagesQueue() {
        return producers[QUEUED_MESSAGES_QUEUE_INDEX];
    }

    public ProducerQueues getAttributeStoreQueue() {
        return producers[ATTRIBUTE_STORE_QUEUE_INDEX];
    }

    public int getPersistenceBucketCount() {
        return persistenceBucketCount;
    }

    public int getCreditsPerExecution() {
        return creditsPerExecution;
    }

    public long getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public AtomicLong getGlobalTaskCount() {
        return globalTaskCount;
    }

    public AtomicLong getNonemptyQueueCounter() {
        return nonemptyQueueCounter;
    }

    public AtomicInteger getRunningThreadsCount() {
        return runningThreadsCount;
    }

    @NotNull
    public ExecutorService[] getCallbackExecutors() {
        return callbackExecutors;
    }

    public void stop() {
        final long start = System.currentTimeMillis();
        if (log.isTraceEnabled()) {
            log.trace("Shutting down single writer");
        }

        singleWriterExecutor.shutdown();

        try {
            singleWriterExecutor.awaitTermination(shutdownGracePeriod, TimeUnit.SECONDS);
            if (log.isTraceEnabled()) {
                log.trace("Finished single writer shutdown in {} ms", (System.currentTimeMillis() - start));
            }
        } catch (final InterruptedException e) {
            //ignore
        }
        singleWriterExecutor.shutdownNow();
        for (final ExecutorService callbackExecutor : callbackExecutors) {
            callbackExecutor.shutdownNow();
        }
        checkScheduler.shutdownNow();
    }

    private static class SingleWriterTask implements Runnable {

        private final AtomicLong nonemptyQueueCounter;
        private final AtomicLong globalTaskCount;
        private final AtomicInteger runningThreadsCount;
        private final ProducerQueues[] producers;
        final int[] probabilities;

        private static final int MIN_PROBABILITY_IN_PERCENT = 5;

        private static final SplittableRandom RANDOM = new SplittableRandom();

        public SingleWriterTask(
                final AtomicLong nonemptyQueueCounter, final AtomicLong globalTaskCount,
                final AtomicInteger runningThreadsCount,
                final ProducerQueues[] producers) {

            this.nonemptyQueueCounter = nonemptyQueueCounter;
            this.globalTaskCount = globalTaskCount;
            this.runningThreadsCount = runningThreadsCount;
            this.producers = producers;
            probabilities = new int[producers.length];
        }

        public void run() {
            try {

                final SplittableRandom random = RANDOM.split();

                // It is possible that all tasks stop running while there are still non-empty queues.
                // We have yet to determine if there is a lock free way to avoid this.
                outerLoop:
                while (nonemptyQueueCounter.get() >= runningThreadsCount.getAndDecrement()) {
                    runningThreadsCount.incrementAndGet();
                    final long countSnapShot = globalTaskCount.get();
                    if (countSnapShot == 0) {
                        continue;
                    }

                    // Calculate the percentage portion of total tasks per persistence.
                    for (int i = 0; i < producers.length; i++) {
                        probabilities[i] = (int) ((producers[i].getTaskCount().get() * 100) / countSnapShot);
                    }

                    int sumWithoutMins = 0;
                    // Set to min probability if necessary
                    for (int i = 0; i < probabilities.length; i++) {
                        if (probabilities[i] < MIN_PROBABILITY_IN_PERCENT) {
                            probabilities[i] = MIN_PROBABILITY_IN_PERCENT;
                        } else {
                            sumWithoutMins += probabilities[i];
                        }
                    }

                    int surplus = 0;
                    for (int i = 0; i < probabilities.length; i++) {
                        surplus += probabilities[i];
                    }
                    surplus -= 100;

                    if (surplus > 0) { // Normalize to a 100% sum
                        // We reduce the probability of all persistences that are not at the minimum, be a portion of the overhead.
                        // The portion is based on there portion of the sum of all probabilities, ignoring those with minimum probability.
                        for (int i = 0; i < probabilities.length; i++) {
                            if (probabilities[i] > MIN_PROBABILITY_IN_PERCENT) {
                                probabilities[i] -= surplus / (sumWithoutMins / probabilities[i]);
                            }
                        }
                    }

                    final int randomInt = random.nextInt(100);
                    int offset = 0;

                    for (int i = 0; i < probabilities.length; i++) {
                        if (randomInt <= probabilities[i] + offset) {
                            producers[i].execute(random);
                            continue outerLoop;
                        }
                        offset += probabilities[i];
                    }
                }

            } catch (final Throwable t) {
                // Exceptions in the executed tasks, are passed to there result future.
                // So we only end up here if there is an exception in the probability calculation.
                // We decrement the running thread count so that a new thread will start running, as soon as a new task is added to any queue.
                runningThreadsCount.decrementAndGet();
                Exceptions.rethrowError("Exception in single writer executor. ", t);
            }
        }
    }

    public interface Task<R> {

        R doTask(int bucketIndex, ImmutableList<Integer> queueBuckets, int queueIndex);
    }

    public interface SuccessCallback<R> {

        void afterTask(R result);
    }

    public interface FailedCallback {

        void afterTask(Exception exception);
    }
}
