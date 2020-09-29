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

package com.hivemq.mqtt.services;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.hivemq.common.shutdown.ShutdownHooks;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.extension.sdk.api.client.parameter.ClientInformation;
import com.hivemq.extension.sdk.api.packets.general.Qos;
import com.hivemq.extension.sdk.api.packets.publish.PayloadFormatIndicator;
import com.hivemq.extension.sdk.api.services.deliver.PublishDeliver;
import com.hivemq.extensions.HiveMQExtension;
import com.hivemq.extensions.HiveMQExtensions;
import com.hivemq.extensions.PluginInformationUtil;
import com.hivemq.extensions.classloader.IsolatedPluginClassloader;
import com.hivemq.extensions.deliver.parameter.PublishDeliverInputImpl;
import com.hivemq.extensions.deliver.parameter.PublishDeliverOutputImpl;
import com.hivemq.extensions.executor.PluginOutPutAsyncer;
import com.hivemq.extensions.executor.PluginTaskExecutorService;
import com.hivemq.extensions.executor.task.PluginInOutTask;
import com.hivemq.extensions.executor.task.PluginInOutTaskContext;
import com.hivemq.extensions.packets.general.UserPropertiesImpl;
import com.hivemq.extensions.services.deliver.Delivers;
import com.hivemq.extensions.services.publish.PublishImpl;
import com.hivemq.mqtt.handler.publish.PublishReturnCode;
import com.hivemq.mqtt.message.mqtt5.MqttUserProperty;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.mqtt.topic.SubscriberWithIdentifiers;
import com.hivemq.mqtt.topic.tree.LocalTopicTree;
import com.hivemq.persistence.RetainedMessage;
import com.hivemq.persistence.deliver.DeliverMessagePersistence;
import com.hivemq.persistence.deliver.DeliverShutdownHook;
import com.hivemq.persistence.deliver.PublishWithSenderDeliver;
import com.hivemq.persistence.retained.RetainedMessagePersistence;
import com.hivemq.persistence.util.FutureUtils;
import com.hivemq.util.Exceptions;
import com.hivemq.util.ThreadFactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hivemq.configuration.service.InternalConfigurations.ACKNOWLEDGE_AFTER_PERSIST;

/**
 * @author Christoph Schäbel
 * @author Dominik Obermaier
 */
@LazySingleton
public class InternalPublishServiceImpl implements InternalPublishService {

    private static final Logger log = LoggerFactory.getLogger(InternalPublishServiceImpl.class);

    private final RetainedMessagePersistence retainedMessagePersistence;
    private final LocalTopicTree topicTree;
    private final PublishDistributor publishDistributor;

    private final boolean acknowledgeAfterPersist;

    private final @NotNull PluginTaskExecutorService executorService;
    private final @NotNull Delivers delivers;
    private final @NotNull PluginOutPutAsyncer asyncer;
    private final @NotNull HiveMQExtensions hiveMQExtensions;
    private final @NotNull DeliverMessagePersistence deliverMessagePersistence;

    private ScheduledExecutorService deliverScheduleService;
    private final @NotNull ShutdownHooks shutdownHooks;

    @Inject
    public InternalPublishServiceImpl(
            final DeliverMessagePersistence deliverMessagePersistence,
            final @NotNull ShutdownHooks shutdownHooks,
            final PluginOutPutAsyncer asyncer,
            final HiveMQExtensions hiveMQExtensions,
            final PluginTaskExecutorService executorService,
            final Delivers delivers,
            final RetainedMessagePersistence retainedMessagePersistence,
            final LocalTopicTree topicTree,
            final PublishDistributor publishDistributor) {
        this.deliverMessagePersistence = deliverMessagePersistence;
        this.shutdownHooks = shutdownHooks;
        this.asyncer = asyncer;
        this.delivers = delivers;
        this.executorService = executorService;
        this.hiveMQExtensions = hiveMQExtensions;
        this.retainedMessagePersistence = retainedMessagePersistence;
        this.topicTree = topicTree;
        this.publishDistributor = publishDistributor;
        this.acknowledgeAfterPersist = ACKNOWLEDGE_AFTER_PERSIST.get();
    }

    @NotNull
    public ListenableFuture<PublishReturnCode> publish(@NotNull final PUBLISH publish, @NotNull final ExecutorService executorService, @Nullable final String sender) {

        Preconditions.checkNotNull(publish, "PUBLISH can not be null");
        Preconditions.checkNotNull(executorService, "executorService can not be null");

        //reset dup-flag
        publish.setDuplicateDelivery(false);

        final ListenableFuture<Void> persistFuture = persistRetainedMessage(publish, executorService);
        final ListenableFuture<PublishReturnCode> publishReturnCodeFuture = handlePublish(publish, executorService, sender);

        //deliver publish
        deliverPublish(sender, publish);

        return Futures.whenAllComplete(publishReturnCodeFuture, persistFuture).call(() -> publishReturnCodeFuture.get(), executorService);
    }

    private ListenableFuture<Void> persistRetainedMessage(final PUBLISH publish, final ExecutorService executorService) {

        //Retained messages need to be persisted and thus we need to make that non-blocking
        if (publish.isRetain()) {

            final SettableFuture<Void> persistSettableFuture = SettableFuture.create();
            final ListenableFuture<Void> persistFuture;
            if (publish.getPayload().length > 0) {
                //pass payloadId null here, because we don't know yet if the message must be stored in the payload persistence
                final RetainedMessage retainedMessage = new RetainedMessage(publish, null, publish.getMessageExpiryInterval());
                log.trace("Adding retained message on topic {}", publish.getTopic());
                persistFuture = retainedMessagePersistence.persist(publish.getTopic(), retainedMessage);

            } else {
                log.trace("Deleting retained message on topic {}", publish.getTopic());
                persistFuture = retainedMessagePersistence.remove(publish.getTopic());
            }

            if (!acknowledgeAfterPersist) {
                persistSettableFuture.set(null);
            } else {

                Futures.addCallback(persistFuture, new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable final Void aVoid) {
                        persistSettableFuture.set(null);
                    }

                    @Override
                    public void onFailure(@NotNull final Throwable throwable) {
                        Exceptions.rethrowError("Unable able to store retained message for topic " + publish.getTopic()
                                + " with message id " + publish.getUniqueId() + ".", throwable);
                        persistSettableFuture.set(null);
                    }

                }, executorService);
            }

            return persistSettableFuture;
        }

        return Futures.immediateFuture(null);
    }

    @NotNull
    private ListenableFuture<PublishReturnCode> handlePublish(@NotNull final PUBLISH publish, @NotNull final ExecutorService executorService, @Nullable final String sender) {

        final ImmutableSet<SubscriberWithIdentifiers> subscribers = topicTree.getSubscribers(publish.getTopic());

        if (subscribers.size() < 1) {
            return Futures.immediateFuture(PublishReturnCode.NO_MATCHING_SUBSCRIBERS);
        }


        if (!acknowledgeAfterPersist) {
            deliverPublish(subscribers, sender, publish, executorService, null);
            return Futures.immediateFuture(PublishReturnCode.DELIVERED);
        }
        final SettableFuture<PublishReturnCode> returnCodeFuture = SettableFuture.create();
        deliverPublish(subscribers, sender, publish, executorService, returnCodeFuture);
        return returnCodeFuture;
    }

    private void deliverPublish(final ImmutableSet<SubscriberWithIdentifiers> subscribers,
                                @Nullable final String sender,
                                @NotNull final PUBLISH publish,
                                @NotNull final ExecutorService executorService,
                                @Nullable final SettableFuture<PublishReturnCode> returnCodeFuture) {
        Set<String> sharedSubscriptions = null;
        final Map<String, SubscriberWithIdentifiers> notSharedSubscribers = new HashMap<>(subscribers.size());

        for (final SubscriberWithIdentifiers subscriber : subscribers) {
            if (!subscriber.isSharedSubscription()) {

                if (subscriber.isNoLocal() && sender != null && sender.equals(subscriber.getSubscriber())) {
                    //do not send to this subscriber, because NoLocal Option is set and subscriber == sender
                    continue;
                }

                notSharedSubscribers.put(subscriber.getSubscriber(), subscriber);
                continue;
            }

            //only instantiate list if shared subscribers are available
            if (sharedSubscriptions == null) {
                sharedSubscriptions = new HashSet<>(subscribers.size());
            }

            sharedSubscriptions.add(subscriber.getSharedName() + "/" + subscriber.getTopicFilter());
        }

        //Send out the messages to the channel of the subscribers
        final ListenableFuture<Void> publishFinishedFutureNonShared = publishDistributor.distributeToNonSharedSubscribers(notSharedSubscribers, publish, executorService);

        final ListenableFuture<Void> publishFinishedFutureShared;
        //Shared subscriptions are currently not batched, since it is unlikely that there are many groups of shared subscribers for the same topic.
        if (sharedSubscriptions != null) {
            publishFinishedFutureShared = publishDistributor.distributeToSharedSubscribers(sharedSubscriptions, publish, executorService);
        } else {
            publishFinishedFutureShared = Futures.immediateFuture(null);
        }

        Futures.addCallback(FutureUtils.mergeVoidFutures(publishFinishedFutureNonShared, publishFinishedFutureShared), new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable final Void result) {
                if (returnCodeFuture != null) {
                    returnCodeFuture.set(PublishReturnCode.DELIVERED);
                }
            }

            @Override
            public void onFailure(@NotNull final Throwable throwable) {
                Exceptions.rethrowError("Unable to publish message for topic " + publish.getTopic() + " with message id" + publish.getUniqueId() + ".", throwable);
                if (returnCodeFuture != null) {
                    returnCodeFuture.set(PublishReturnCode.FAILED);
                }
            }
        }, executorService);
    }

    @NotNull
    private Set<String> deliverPublish(@Nullable final String sender, @NotNull final PUBLISH publish) {

        Map<String, PublishDeliver> publishDeliverMap = delivers.getPublishDeliverMap();
        if (publishDeliverMap == null || publishDeliverMap.isEmpty()) {
            return Set.of();
        }

        ClientInformation clientInformation = PluginInformationUtil.getClientInformation(sender);

        PayloadFormatIndicator indicator = publish.getPayloadFormatIndicator() == null ?
                null : PayloadFormatIndicator.valueOf(publish.getPayloadFormatIndicator().name());
        ByteBuffer correlationData =
                publish.getCorrelationData() == null ? null : ByteBuffer.wrap(publish.getCorrelationData());

        ByteBuffer payload = publish.getPayload() == null ? null : ByteBuffer.wrap(publish.getPayload());

        ImmutableList.Builder<MqttUserProperty> userPropertyBuilder = ImmutableList.builder();

        if (publish.getUserProperties() != null) {
            for (MqttUserProperty property : publish.getUserProperties().asList()) {
                userPropertyBuilder.add(property);
            }
        }

        PublishImpl deliver = new PublishImpl(Qos.valueOf(publish.getQoS().name()),
                publish.isRetain(),
                publish.getTopic(),
                indicator,
                publish.getMessageExpiryInterval(),
                publish.getResponseTopic(),
                correlationData,
                publish.getContentType(),
                payload,
                UserPropertiesImpl.of(userPropertyBuilder.build()));
        PublishDeliverInputImpl input = new PublishDeliverInputImpl(clientInformation, deliver);
        PublishDeliverOutputImpl output = new PublishDeliverOutputImpl(asyncer);
        Set<String> expectDelivers = (publish instanceof PublishWithSenderDeliver) ?
                ((PublishWithSenderDeliver) publish).getDelivers() : publishDeliverMap.keySet();

        Map<String, PublishDeliver> validDeliverMap = new HashMap<>();
        for (String expectDeliver: expectDelivers) {
            PublishDeliver publishDeliver = publishDeliverMap.get(expectDeliver);
            final HiveMQExtension extension = hiveMQExtensions.getExtensionForClassloader(
                    (IsolatedPluginClassloader) publishDeliver.getClass().getClassLoader());
            if (extension == null) {
                continue;
            }
            validDeliverMap.put(extension.getId(), publishDeliver);
        }

        PublishDeliverContext context = new PublishDeliverContext(sender,
                Sets.newHashSet(validDeliverMap.keySet()), publish, deliverMessagePersistence);
        for (Map.Entry<String, PublishDeliver> entry : validDeliverMap.entrySet()) {
            final PublishDeliverTask task = new PublishDeliverTask(entry.getValue(), entry.getKey());
            executorService.handlePluginInOutTaskExecution(context, input, output, task);
        }
        if (!(publish instanceof PublishWithSenderDeliver)) {
            return Set.of();
        }
        try {
            boolean postCompleteSuccess = output.getPostFuture().get();
            if (!postCompleteSuccess) {
                //error occur, some deliver not success, we can`t persist message, so message will never remove
                return Set.of();
            }
        } catch (Throwable t) {
            //do nothing, just wait complete
        }
        return output.getCompleteDelivers();
    }

    private static class PublishDeliverContext extends PluginInOutTaskContext<PublishDeliverOutputImpl> {

        private int deliverCount;

        private final @NotNull AtomicInteger counter;

        private final Set<String> delivers;

        private final PUBLISH publish;

        private final DeliverMessagePersistence deliverMessagePersistence;

        protected PublishDeliverContext(
                String identifier,
                Set<String> delivers,
                PUBLISH publish,
                DeliverMessagePersistence deliverMessagePersistence) {
            super(identifier);
            this.delivers = delivers;
            this.deliverCount = delivers.size();
            this.counter = new AtomicInteger(0);
            this.publish = publish;
            this.deliverMessagePersistence = deliverMessagePersistence;
        }

        @Override
        public void pluginPost(@NotNull PublishDeliverOutputImpl pluginOutput) {
            if (counter.incrementAndGet() == deliverCount) {
                try {
                    delivers.removeAll(pluginOutput.getCompleteDelivers());
                    if (delivers.size() == 0) {
                        return;
                    }
                    PublishWithSenderDeliver reDeliver;
                    if (publish instanceof PublishWithSenderDeliver) {
                        if (pluginOutput.getCompleteDelivers().size() == 0) {
                            return;
                        }
                        PublishWithSenderDeliver publishWith = (PublishWithSenderDeliver) publish;
                        Set<String> failDelivers = Set.copyOf(Sets.difference(
                                publishWith.getDelivers(),
                                pluginOutput.getCompleteDelivers()));
                        publishWith.setDelivers(failDelivers);
                        reDeliver = publishWith;
                    } else {
                        reDeliver = new PublishWithSenderDeliver(publish, getIdentifier(), delivers);
                    }
                    deliverMessagePersistence.persist(reDeliver);
                } catch (Throwable t) {
                    log.warn("deliver persist exception, message will deliver more times");
                    pluginOutput.getPostFuture().set(false);
                } finally {
                    pluginOutput.getPostFuture().set(true);
                }
            }
        }
    }

    private static class PublishDeliverTask implements
            PluginInOutTask<PublishDeliverInputImpl, PublishDeliverOutputImpl> {

        private final @NotNull PublishDeliver publishDeliver;
        private final @NotNull String extensionId;

        PublishDeliverTask(final @NotNull PublishDeliver publishDeliver, final @NotNull String extensionId) {
            this.publishDeliver = publishDeliver;
            this.extensionId = extensionId;
        }

        @Override
        public @NotNull ClassLoader getPluginClassLoader() {
            return publishDeliver.getClass().getClassLoader();
        }

        @Override
        public PublishDeliverOutputImpl apply(
                PublishDeliverInputImpl publishDeliverInput,
                PublishDeliverOutputImpl publishDeliverOutput) {
            try {
                publishDeliver.deliver(publishDeliverInput, publishDeliverOutput);
                publishDeliverOutput.addComplete(extensionId);
            } catch (final Throwable e) {
                log.warn("Uncaught exception was thrown from extension with id \"{}\" on outbound publish deliver. " +
                                "Extensions are responsible for their own exception handling.", extensionId);
                log.debug("Original Exception: ", e);
                Exceptions.rethrowError(e);
            }
            return publishDeliverOutput;
        }
    }

    public void startDeliverSchedule() {
        log.info("Start Deliver Schedule ...");
        Map<String, PublishDeliver> publishDeliverMap = delivers.getPublishDeliverMap();
        if (publishDeliverMap == null || publishDeliverMap.isEmpty()) {
            return;
        }
        log.info("Start Deliver Schedule {}", publishDeliverMap.keySet());
        final ThreadFactory threadFactory = ThreadFactoryUtil.create("deliver-message-schedule-%d");
        deliverScheduleService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        shutdownHooks.add(new DeliverShutdownHook(deliverScheduleService));
        deliverScheduleService.scheduleAtFixedRate(() -> {
            try {
                deliverMessagePersistence.iterate((DeliverMessagePersistence.IterationContext context,
                        PublishWithSenderDeliver publishWith) -> {
                    if (!deliverScheduleService.isShutdown()) {
                        Set<String> beforeDelivers = Set.copyOf(publishWith.getDelivers());
                        Set<String> delivers = deliverPublish(publishWith.getSender(), publishWith);
                        if (beforeDelivers.equals(delivers)) {
                            deliverMessagePersistence.remove(publishWith.getDeliverId());
                        }
                    } else {
                        context.abortIteration();
                    }
                });
            } catch (Throwable t) {
                log.error("deliver schedule error", t);
            }
        }, 0, 100L, TimeUnit.MILLISECONDS);
    }
}
