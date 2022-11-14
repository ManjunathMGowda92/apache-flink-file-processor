package org.manjunath.flink.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.springframework.util.SerializationUtils;

import java.io.IOException;

public class AppDeserializer<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 6283306952012323805L;

    @Override
    public T deserialize(byte[] message) throws IOException {
        return (T) SerializationUtils.deserialize(message);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        Class<? extends AppDeserializer> aClass = getClass();
        return (TypeInformation<T>) TypeInformation.of(aClass);
    }
}
