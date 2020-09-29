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
package com.hivemq.persistence.deliver;

import com.hivemq.bootstrap.ioc.lazysingleton.LazySingleton;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.migration.meta.PersistenceType;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * The persistence provider for retained messages.
 *
 * @author Dominik Obermaier
 * @author Florian Limpöck
 */
@LazySingleton
public class DeliverMessageLocalPersistenceProvider implements Provider<DeliverMessageLocalPersistence> {

    private final @NotNull Provider<DeliverMessageRocksDBLocalPersistence> rocksDBProvider;
    private final @NotNull Provider<DeliverMessageXodusLocalPersistence> xodusProvider;
    private final @NotNull PersistenceType persistenceType;

    @Inject
    public DeliverMessageLocalPersistenceProvider(final @NotNull Provider<DeliverMessageRocksDBLocalPersistence> rocksDBProvider,
                                                   final @NotNull Provider<DeliverMessageXodusLocalPersistence> xodusProvider) {
        this.rocksDBProvider = rocksDBProvider;
        this.xodusProvider = xodusProvider;
        this.persistenceType = InternalConfigurations.DELIVER_MESSAGE_PERSISTENCE_TYPE.get();
    }

    @NotNull
    @Override
    public DeliverMessageLocalPersistence get() {
        if(persistenceType == PersistenceType.FILE_NATIVE) {
            return rocksDBProvider.get();
        } else {
            return xodusProvider.get();
        }
    }

}
