package com.hivemq.persistence.deliver;

import com.hivemq.mqtt.message.QoS;
import com.hivemq.mqtt.message.publish.PUBLISH;
import com.hivemq.mqtt.message.publish.PUBLISHFactory;
import com.hivemq.persistence.payload.PublishPayloadPersistence;
import io.netty.handler.codec.spdy.SpdyHttpResponseStreamIdHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Set;

import static org.junit.Assert.*;

public class DeliverMessageXodusSerializerTest {

    @Mock
    private PublishPayloadPersistence payloadPersistence;

    private DeliverMessageXodusSerializer serializer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        serializer = new DeliverMessageXodusSerializer(payloadPersistence);
    }

    @Test
    public void serializeValue() {
        String sender = "sender";
        Set<String> delivers = Set.of("deliver", "deliver1");

        final PUBLISHFactory.Mqtt5Builder builder = new PUBLISHFactory.Mqtt5Builder();
        builder.withHivemqId("abc");
        builder.withTopic("feng/xiang");
        builder.withQoS(QoS.AT_LEAST_ONCE);
        builder.withTimestamp(123456);
        builder.withPayload(new byte[0]);
        builder.withPayloadId(1L);
        builder.withPersistence(payloadPersistence);
        PUBLISH publish = builder.build();
        PublishWithSenderDeliver publishWith = new PublishWithSenderDeliver(publish, sender, delivers);

        byte[] valueBytes = serializer.serializeValue(publishWith);
        PublishWithSenderDeliver publishWithD = serializer.deserializeValue(valueBytes);

        System.out.println(publishWith);
        System.out.println(publishWithD);
        System.out.println(publishWith.getDelivers());
        System.out.println(publishWithD.getDelivers());
        assertEquals(sender, publishWithD.getSender());
        assertEquals(delivers, publishWithD.getDelivers());
    }
}