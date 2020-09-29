package com.hivemq.extensions.services.deliver;

import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.ThreadSafe;
import com.hivemq.extension.sdk.api.services.deliver.PublishDeliver;
import com.hivemq.extensions.HiveMQExtension;
import com.hivemq.extensions.HiveMQExtensions;
import com.hivemq.extensions.PluginPriorityComparator;
import com.hivemq.extensions.classloader.IsolatedPluginClassloader;

import javax.inject.Inject;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@LazySingleton
@ThreadSafe
public class DeliversImpl implements Delivers {
    @NotNull
    private final Map<@NotNull String, @NotNull PublishDeliver> publishDeliverMap;

    @NotNull
    private final ReadWriteLock readWriteLock;

    @NotNull
    private final HiveMQExtensions hiveMQExtensions;

    @Inject
    public DeliversImpl(@NotNull final HiveMQExtensions hiveMQExtensions) {
        this.hiveMQExtensions = hiveMQExtensions;
        this.publishDeliverMap = new TreeMap<>(new PluginPriorityComparator(hiveMQExtensions));
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    @Override
    public void addPublishDeliver(@NotNull final PublishDeliver deliver) {

        final Lock writeLock = readWriteLock.writeLock();

        writeLock.lock();

        try {

            final IsolatedPluginClassloader pluginClassloader =
                    (IsolatedPluginClassloader) deliver.getClass().getClassLoader();

            final HiveMQExtension plugin = hiveMQExtensions.getExtensionForClassloader(pluginClassloader);

            if (plugin != null) {

                publishDeliverMap.put(plugin.getId(), deliver);
            }

        } finally {
            writeLock.unlock();
        }
    }

    @NotNull
    public Map<@NotNull String, @NotNull PublishDeliver> getPublishDeliverMap() {

        final Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            return publishDeliverMap;
        } finally {
            readLock.unlock();
        }
    }
}
