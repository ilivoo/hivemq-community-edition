package com.hivemq.persistence.deliver;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.persistence.LocalPersistence;

import java.util.List;

public interface DeliverMessageLocalPersistence extends LocalPersistence {

    String PERSISTENCE_NAME = "deliver_messages";

    /**
     * Due to concurrent access to the persistence, this value may not be correct.
     *
     * @return The amount of all retained messages stored in the persistence.
     */
    long size();

    void put(PublishWithSenderDeliver publishWith, int bucketIndex);

    PublishWithSenderDeliver get(long payloadId, int bucketIndex);

    void remove(long payloadId, int bucketIndex);

    void clear(int bucketIndex);

    List<Long> getIds(int bucketIndex);

    void iterate(@NotNull ItemCallback callBack);

    interface ItemCallback {

        void onItem(@NotNull PublishWithSenderDeliver publishWith);
    }
}
