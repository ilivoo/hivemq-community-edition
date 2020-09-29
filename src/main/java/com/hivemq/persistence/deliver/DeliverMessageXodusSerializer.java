package com.hivemq.persistence.deliver;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.hivemq.codec.encoder.mqtt5.Mqtt5PayloadFormatIndicator;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.mqtt5.Mqtt5UserProperties;
import com.hivemq.mqtt.message.mqtt5.PropertiesSerializationUtil;
import com.hivemq.mqtt.message.publish.PUBLISHFactory;
import com.hivemq.persistence.local.xodus.XodusUtils;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import com.hivemq.util.Bytes;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DeliverMessageXodusSerializer {

    private static final byte PUBLISH_BIT = (byte) 0b1000_0000;
    private static final byte RETAINED_BIT = (byte) 0b0010_0000;
    private static final byte DUPLICATE_DELIVERY_BIT = (byte) 0b0001_0000;

    private static final byte QOS_BITS = (byte) 0b0000_0011;

    private static final byte RESPONSE_TOPIC_PRESENT_BIT = (byte) 0b1000_0000;
    private static final byte CONTENT_TYPE_PRESENT_BIT = (byte) 0b0100_0000;
    private static final byte CORRELATION_DATA_PRESENT_BIT = (byte) 0b0010_0000;
    private static final byte USER_PROPERTIES_PRESENT_BIT = (byte) 0b0000_1000;
    private static final byte DELIVERS_PRESENT_BIT = (byte) 0b0000_0100;

    private @NotNull final PublishPayloadPersistence payloadPersistence;

    DeliverMessageXodusSerializer(final @NotNull PublishPayloadPersistence payloadPersistence) {
        this.payloadPersistence = payloadPersistence;
    }

    @NotNull
    public byte[] serializeKey(final long payloadId) {
        final byte[] bytes = new byte[8];
        Bytes.copyLongToByteArray(payloadId, bytes, 0);
        return bytes;
    }

    @NotNull
    public long deserializeKey(final @NotNull byte[] serialized) {
        checkNotNull(serialized, "Byte array must not be null");
        return Bytes.readLong(serialized, 0);
    }

    public byte[] serializeValue(PublishWithSenderDeliver publishWith) {
        final byte[] sender = publishWith.getSender().getBytes(UTF_8);
        final byte[] topic = publishWith.getTopic().getBytes(UTF_8);
        final byte[] hivemqId = publishWith.getHivemqId().getBytes(UTF_8);
        final byte[] responseTopic = publishWith.getResponseTopic() == null ? null : publishWith.getResponseTopic().getBytes(UTF_8);
        final byte[] contentType = publishWith.getContentType() == null ? null : publishWith.getContentType().getBytes(UTF_8);
        final byte[] correlationData = publishWith.getCorrelationData();
        final int payloadFormatIndicator = publishWith.getPayloadFormatIndicator() != null ? publishWith.getPayloadFormatIndicator().getCode() : -1;
        final Mqtt5UserProperties userProperties = publishWith.getUserProperties();
        final Set<String> delivers = publishWith.getDelivers();

        final byte[] result = new byte[
                        1 + // PUBLISH_BIT, dup, retain, qos
                        1 + // present flags
                        XodusUtils.shortLengthArraySize(topic) + // topic
                        XodusUtils.shortLengthArraySize(sender) + // topic
                        Long.BYTES + // timestamp
                        Long.BYTES + // publish id
                        XodusUtils.shortLengthArraySize(hivemqId) + // hivemq id
                        Long.BYTES + // payload id
                        Long.BYTES + // message expiry
                        (responseTopic == null ? 0 : XodusUtils.shortLengthArraySize(responseTopic)) + // response topic
                        (contentType == null ? 0 : XodusUtils.shortLengthArraySize(contentType)) + // content type
                        (correlationData == null ? 0 : XodusUtils.shortLengthArraySize(correlationData)) + // correlation data

                        1 + // payload format indicator
                        (userProperties.asList().size() == 0 ? 0 : PropertiesSerializationUtil.encodedSize(userProperties)) +
                        ((delivers == null && delivers.size() == 0) ? 0 : setEncodedSize(publishWith.getDelivers())) //delivers
                ];

        int cursor = 0;

        byte flags = PUBLISH_BIT;
        flags |= publishWith.getQoS().getQosNumber();
        if (publishWith.isDuplicateDelivery()) {
            flags |= DUPLICATE_DELIVERY_BIT;
        }
        if (publishWith.isRetain()) {
            flags |= RETAINED_BIT;
        }
        cursor = XodusUtils.serializeByte(flags, result, cursor);

        byte presentFlags = (byte) 0b0000_0000;

        if (responseTopic != null) {
            presentFlags |= RESPONSE_TOPIC_PRESENT_BIT;
        }
        if (contentType != null) {
            presentFlags |= CONTENT_TYPE_PRESENT_BIT;
        }
        if (correlationData != null) {
            presentFlags |= CORRELATION_DATA_PRESENT_BIT;
        }
        if (userProperties.asList().size() > 0) {
            presentFlags |= USER_PROPERTIES_PRESENT_BIT;
        }
        if (delivers != null && delivers.size() > 0) {
            presentFlags |= DELIVERS_PRESENT_BIT;
        }

        cursor = XodusUtils.serializeByte(presentFlags, result, cursor);

        cursor = XodusUtils.serializeShortLengthArray(topic, result, cursor);
        cursor = XodusUtils.serializeShortLengthArray(sender, result, cursor);
        cursor = XodusUtils.serializeLong(publishWith.getTimestamp(), result, cursor);
        cursor = XodusUtils.serializeLong(publishWith.getLocalPublishId(), result, cursor);
        cursor = XodusUtils.serializeShortLengthArray(hivemqId, result, cursor);
        cursor = XodusUtils.serializeLong(publishWith.getPayloadId(), result, cursor);
        cursor = XodusUtils.serializeLong(publishWith.getMessageExpiryInterval(), result, cursor);
        if (responseTopic != null) {
            cursor = XodusUtils.serializeShortLengthArray(responseTopic, result, cursor);
        }
        if (contentType != null) {
            cursor = XodusUtils.serializeShortLengthArray(contentType, result, cursor);
        }
        if (correlationData != null) {
            cursor = XodusUtils.serializeShortLengthArray(correlationData, result, cursor);
        }

        cursor = XodusUtils.serializeByte((byte) payloadFormatIndicator, result, cursor);
        if (userProperties.asList().size() > 0) {
            cursor = PropertiesSerializationUtil.write(userProperties, result, cursor);
        }
        if (delivers != null && delivers.size() > 0) {
            writeSet(delivers, result, cursor);
        }
        return result;
    }

    private static int setEncodedSize(final Set<String> set) {
        int size = Integer.BYTES; // List size integer
        for (String s : set) {
            size += XodusUtils.shortLengthStringSize(s);
        }
        return size;
    }

    public static int writeSet(Set<String> set, @NotNull final byte[] bytes, int offset) {
        Bytes.copyIntToByteArray(set.size(), bytes, offset);
        offset += Integer.BYTES;
        for (String s : set) {
            offset = XodusUtils.serializeShortLengthString(s, bytes, offset);
        }
        return offset;
    }

    public PublishWithSenderDeliver deserializeValue(byte[] serialized) {
        final PUBLISHFactory.Mqtt5Builder builder = new PUBLISHFactory.Mqtt5Builder();

        int cursor = 0;

        builder.withQoS(QoS.valueOf(serialized[cursor] & QOS_BITS));
        builder.withDuplicateDelivery((serialized[cursor] & DUPLICATE_DELIVERY_BIT) == DUPLICATE_DELIVERY_BIT);
        builder.withRetain((serialized[cursor] & RETAINED_BIT) == RETAINED_BIT);
        cursor += 1;

        final boolean responseTopicPresent = (serialized[cursor] & RESPONSE_TOPIC_PRESENT_BIT) == RESPONSE_TOPIC_PRESENT_BIT;
        final boolean contentTypePresent = (serialized[cursor] & CONTENT_TYPE_PRESENT_BIT) == CONTENT_TYPE_PRESENT_BIT;
        final boolean correlationDataPresent = (serialized[cursor] & CORRELATION_DATA_PRESENT_BIT) == CORRELATION_DATA_PRESENT_BIT;
        final boolean userPropertiesPresent = (serialized[cursor] & USER_PROPERTIES_PRESENT_BIT) == USER_PROPERTIES_PRESENT_BIT;
        final boolean deliversPresent = (serialized[cursor] & DELIVERS_PRESENT_BIT) == DELIVERS_PRESENT_BIT;
        cursor += 1;

        final int topicLength = Bytes.readUnsignedShort(serialized, cursor);
        cursor += Short.BYTES;
        builder.withTopic(new String(serialized, cursor, topicLength, UTF_8));
        cursor += topicLength;

        final int senderLength = Bytes.readUnsignedShort(serialized, cursor);
        cursor += Short.BYTES;
        String sender = new String(serialized, cursor, senderLength, UTF_8);
        cursor += senderLength;

        builder.withTimestamp(Bytes.readLong(serialized, cursor));
        cursor += Long.BYTES;

        builder.withPublishId(Bytes.readLong(serialized, cursor));
        cursor += Long.BYTES;

        final int hivemqIdLength = Bytes.readUnsignedShort(serialized, cursor);
        cursor += Short.BYTES;
        builder.withHivemqId(new String(serialized, cursor, hivemqIdLength, UTF_8));
        cursor += hivemqIdLength;

        builder.withPayloadId(Bytes.readLong(serialized, cursor));
        cursor += Long.BYTES;

        builder.withMessageExpiryInterval(Bytes.readLong(serialized, cursor));
        cursor += Long.BYTES;

        if (responseTopicPresent) {
            final int responseTopicLength = Bytes.readUnsignedShort(serialized, cursor);
            cursor += Short.BYTES;
            if (responseTopicLength != 0) {
                builder.withResponseTopic(new String(serialized, cursor, responseTopicLength, UTF_8));
                cursor += responseTopicLength;
            }
        }

        if (contentTypePresent) {
            final int contentTypeLength = Bytes.readUnsignedShort(serialized, cursor);
            cursor += Short.BYTES;
            if (contentTypeLength != 0) {
                builder.withContentType(new String(serialized, cursor, contentTypeLength, UTF_8));
                cursor += contentTypeLength;
            }
        }

        if (correlationDataPresent) {
            final int correlationDataLength = Bytes.readUnsignedShort(serialized, cursor);
            cursor += Short.BYTES;
            if (correlationDataLength != 0) {
                final byte[] correlationData = new byte[correlationDataLength];
                System.arraycopy(serialized, cursor, correlationData, 0, correlationDataLength);
                builder.withCorrelationData(correlationData);
                cursor += correlationDataLength;
            }
        }

        builder.withPayloadFormatIndicator(Mqtt5PayloadFormatIndicator.fromCode(serialized[cursor]));
        cursor += 1;

        if (userPropertiesPresent) {
            builder.withUserProperties(PropertiesSerializationUtil.read(serialized, cursor));
        }
        Set<String> delivers = null;
        if (deliversPresent) {
            delivers = readSet(serialized, cursor);
        }
        builder.withPersistence(payloadPersistence);
        return new PublishWithSenderDeliver(builder.build(), sender, delivers);
    }

    public static Set<String> readSet(final byte[] bytes, int offset) {
        final int size = Bytes.readInt(bytes, offset);
        offset += Integer.BYTES;
        final ImmutableSet.Builder<String> builder = ImmutableSet.builderWithExpectedSize(size);

        for (int i = 0; i < size; i++) {
            final int length = Bytes.readUnsignedShort(bytes, offset);
            offset += Short.BYTES;
            final String value = new String(bytes, offset, length, Charsets.UTF_8);
            offset += length;
            builder.add(value);
        }
        return builder.build();
    }
}
