package org.manjunath.flink.deserializer;

import org.manjunath.flink.model.MessageInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.springframework.util.SerializationUtils;

import java.io.IOException;

/**
 * Deserializer class needed for the Flink Kafka Consumer
 * i.e {@link org.apache.flink.connector.kafka.source.KafkaSource}
 */
public class MessageInfoDeserializer implements DeserializationSchema<MessageInfo> {
    private static final long serialVersionUID = -4076058336514697110L;

    @Override
    public MessageInfo deserialize(byte[] message) throws IOException {
        return (MessageInfo) SerializationUtils.deserialize(message);
    }

    @Override
    public boolean isEndOfStream(MessageInfo nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MessageInfo> getProducedType() {
        return TypeInformation.of(MessageInfo.class);
    }
}
