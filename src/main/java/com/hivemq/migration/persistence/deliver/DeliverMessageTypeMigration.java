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
package com.hivemq.migration.persistence.deliver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.hivemq.configuration.info.SystemInformation;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.migration.Migrations;
import com.hivemq.migration.TypeMigration;
import com.hivemq.migration.logging.PayloadExceptionLogging;
import com.hivemq.migration.meta.MetaFileService;
import com.hivemq.migration.meta.MetaInformation;
import com.hivemq.migration.meta.PersistenceType;
import com.hivemq.persistence.deliver.DeliverMessageLocalPersistence;
import com.hivemq.persistence.deliver.DeliverMessageRocksDBLocalPersistence;
import com.hivemq.persistence.deliver.DeliverMessageXodusLocalPersistence;
import com.hivemq.persistence.deliver.PublishWithSenderDeliver;
import com.hivemq.persistence.local.xodus.bucket.BucketUtils;
import com.hivemq.persistence.payload.PayloadPersistenceException;
import com.hivemq.persistence.payload.PublishPayloadLocalPersistence;
import com.hivemq.persistence.payload.PublishPayloadLocalPersistenceProvider;
import com.hivemq.util.Exceptions;
import com.hivemq.util.LocalPersistenceFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.File;

/**
 * @author Florian Limpöck
 */
public class DeliverMessageTypeMigration implements TypeMigration {

    private static final Logger log = LoggerFactory.getLogger(DeliverMessageTypeMigration.class);
    private static final Logger migrationLog = LoggerFactory.getLogger(Migrations.MIGRATION_LOGGER_NAME);
    private static final String FIRST_BUCKET_FOLDER = "deliver_messages_0";

    private final @NotNull Provider<DeliverMessageXodusLocalPersistence> xodusLocalPersistenceProvider;
    private final @NotNull Provider<DeliverMessageRocksDBLocalPersistence> rocksDBLocalPersistenceProvider;
    private final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil;
    private final @NotNull PublishPayloadLocalPersistenceProvider publishPayloadLocalPersistenceProvider;
    private final @NotNull SystemInformation systemInformation;
    private final @NotNull PayloadExceptionLogging payloadExceptionLogging;

    private final int bucketCount;
    private final @NotNull PersistenceType configuredType;

    @Inject
    public DeliverMessageTypeMigration(final @NotNull LocalPersistenceFileUtil localPersistenceFileUtil,
            final @NotNull Provider<DeliverMessageXodusLocalPersistence> xodusLocalPersistenceProvider,
            final @NotNull Provider<DeliverMessageRocksDBLocalPersistence> rocksDBLocalPersistenceProvider,
            final @NotNull PublishPayloadLocalPersistenceProvider publishPayloadLocalPersistenceProvider,
            final @NotNull SystemInformation systemInformation,
            final @NotNull PayloadExceptionLogging payloadExceptionLogging) {
        this.localPersistenceFileUtil = localPersistenceFileUtil;
        this.xodusLocalPersistenceProvider = xodusLocalPersistenceProvider;
        this.rocksDBLocalPersistenceProvider = rocksDBLocalPersistenceProvider;
        this.publishPayloadLocalPersistenceProvider = publishPayloadLocalPersistenceProvider;
        this.systemInformation = systemInformation;
        this.bucketCount = InternalConfigurations.PERSISTENCE_BUCKET_COUNT.get();
        this.payloadExceptionLogging = payloadExceptionLogging;
        this.configuredType = InternalConfigurations.DELIVER_MESSAGE_PERSISTENCE_TYPE.get();
    }

    @Override
    public void migrateToType(final @NotNull PersistenceType type) {
        if (type.equals(PersistenceType.FILE_NATIVE)) {
            migrateToRocksDB();
        } else if(type.equals(PersistenceType.FILE)) {
            migrateToXodus();
        } else {
            throw new IllegalArgumentException("Unknown persistence type " + type + " for deliver message migration");
        }
    }

    private void migrateToXodus() {

        final File persistenceFolder = localPersistenceFileUtil.getVersionedLocalPersistenceFolder(DeliverMessageRocksDBLocalPersistence.PERSISTENCE_NAME, DeliverMessageRocksDBLocalPersistence.PERSISTENCE_VERSION);
        if (oldFolderMissing(persistenceFolder)){
            return;
        }

        final DeliverMessageXodusLocalPersistence xodus = xodusLocalPersistenceProvider.get();
        final DeliverMessageRocksDBLocalPersistence rocks = rocksDBLocalPersistenceProvider.get();
        final PublishPayloadLocalPersistence publishPayloadLocalPersistence = publishPayloadLocalPersistenceProvider.get();

        rocks.iterate(new DeliverMessagePersistenceTypeSwitchCallback(bucketCount, publishPayloadLocalPersistence, xodus, payloadExceptionLogging));

        savePersistenceType(PersistenceType.FILE);

        rocks.stop();

    }

    private void migrateToRocksDB() {

        final File persistenceFolder = localPersistenceFileUtil.getVersionedLocalPersistenceFolder(DeliverMessageXodusLocalPersistence.PERSISTENCE_NAME, DeliverMessageXodusLocalPersistence.PERSISTENCE_VERSION);
        if (oldFolderMissing(persistenceFolder)){
            return;
        }

        final DeliverMessageXodusLocalPersistence xodus = xodusLocalPersistenceProvider.get();
        final DeliverMessageRocksDBLocalPersistence rocks = rocksDBLocalPersistenceProvider.get();
        final PublishPayloadLocalPersistence publishPayloadLocalPersistence = publishPayloadLocalPersistenceProvider.get();

        xodus.iterate(new DeliverMessagePersistenceTypeSwitchCallback(bucketCount, publishPayloadLocalPersistence, rocks, payloadExceptionLogging));

        savePersistenceType(PersistenceType.FILE_NATIVE);

        xodus.stop();
    }

    private boolean oldFolderMissing(final @NotNull File persistenceFolder) {
        final File oldPersistenceFolder = new File(persistenceFolder, FIRST_BUCKET_FOLDER);
        if (!oldPersistenceFolder.exists()) {
            migrationLog.info("No (old) persistence folder (deliver_messages) present, skipping migration.");
            log.debug("No (old) persistence folder (deliver_messages) present, skipping migration.");
            return true;
        }
        return false;
    }

    private void savePersistenceType(final @NotNull PersistenceType persistenceType) {
        final MetaInformation metaFile = MetaFileService.readMetaFile(systemInformation);
        metaFile.setDeliverMessagesPersistenceType(persistenceType);
        metaFile.setDeliverMessagesPersistenceVersion(persistenceType == PersistenceType.FILE_NATIVE ? DeliverMessageRocksDBLocalPersistence.PERSISTENCE_VERSION : DeliverMessageXodusLocalPersistence.PERSISTENCE_VERSION);
        MetaFileService.writeMetaFile(systemInformation, metaFile);
    }

    private boolean checkPreviousType(final @NotNull PersistenceType persistenceType) {

        final MetaInformation metaInformation = MetaFileService.readMetaFile(systemInformation);
        final PersistenceType metaType = metaInformation.getDeliverMessagesPersistenceType();

        if (metaType != null && metaType.equals(persistenceType)) {
            //should never happen since getNeededMigrations() will skip those.
            migrationLog.info("Deliver message persistence is already migrated to current type {}, skipping migration", persistenceType);
            log.debug("Deliver message persistence is already migrated to current type {}, skipping migration", persistenceType);
            return false;
        }
        return true;
    }


    @VisibleForTesting
    static class DeliverMessagePersistenceTypeSwitchCallback implements DeliverMessageLocalPersistence.ItemCallback {

        private final int bucketCount;
        private final @NotNull PublishPayloadLocalPersistence payloadLocalPersistence;
        private final @NotNull DeliverMessageLocalPersistence deliverMessageLocalPersistence;
        private final @NotNull PayloadExceptionLogging payloadExceptionLogging;

        DeliverMessagePersistenceTypeSwitchCallback(final int bucketCount,
                final @NotNull PublishPayloadLocalPersistence payloadLocalPersistence,
                final @NotNull DeliverMessageLocalPersistence deliverMessageLocalPersistence,
                final @NotNull PayloadExceptionLogging payloadExceptionLogging) {
            this.bucketCount = bucketCount;
            this.payloadLocalPersistence = payloadLocalPersistence;
            this.deliverMessageLocalPersistence = deliverMessageLocalPersistence;
            this.payloadExceptionLogging = payloadExceptionLogging;
        }

        @Override
        public void onItem(final @NotNull PublishWithSenderDeliver publishWith) {
            @Nullable Long deliverId = publishWith.getDeliverId();
            Preconditions.checkNotNull(deliverId, "Deliver ID must never be null here");
            try {
                final int bucketIndex = BucketUtils.getBucket(deliverId.toString(), bucketCount);
                deliverMessageLocalPersistence.put(publishWith, bucketIndex);

            } catch (final PayloadPersistenceException payloadException) {
                payloadExceptionLogging.addLogging(deliverId, "deliver", publishWith.isRetain(), publishWith.getTopic());
            } catch (final Throwable throwable) {
                log.warn("Could not migrate deliver message with payload id {}, original exception: ", deliverId, throwable);
                Exceptions.rethrowError(throwable);
            }
        }
    }
}
