package io.apicurio.registry.test;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.apicurio.registry.demo.domain.Log;
import io.apicurio.registry.utils.serde.AbstractKafkaSerDe;
import io.apicurio.registry.utils.serde.AbstractKafkaStrategyAwareSerDe;
import io.apicurio.registry.utils.serde.ProtobufKafkaDeserializer;
import io.apicurio.registry.utils.serde.ProtobufKafkaSerializer;
import io.apicurio.registry.utils.serde.strategy.GetOrCreateIdStrategy;
import io.apicurio.registry.utils.serde.util.HeaderUtils;
import io.apicurio.registry.utils.serde.util.Utils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ales Justin
 */
public class I1186Main {
    public static void main(String[] args) throws Exception {
        //configure producer
        Map<String, Object> producerParams = new HashMap<>();
        producerParams.put(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, "http://localhost:8080/api");
        producerParams.put(AbstractKafkaStrategyAwareSerDe.REGISTRY_GLOBAL_ID_STRATEGY_CONFIG_PARAM, GetOrCreateIdStrategy.class);
        producerParams.put(AbstractKafkaSerDe.USE_HEADERS, true); //global Id location

        ProtobufKafkaSerializerWithHeaders<Log.LogMerge> serializer = new ProtobufKafkaSerializerWithHeaders<>();
        serializer.configure(producerParams, false);

        //configure consumer
        Map<String, Object> consumerParams = new HashMap<>();
        consumerParams.put(AbstractKafkaSerDe.REGISTRY_URL_CONFIG_PARAM, "http://localhost:8080/api");
        consumerParams.put(AbstractKafkaSerDe.USE_HEADERS, true); //global Id location

        ProtobufKafkaDeserializerWithHeaders deserializer = new ProtobufKafkaDeserializerWithHeaders();
        deserializer.configure(consumerParams, false);

        Log.LogMerge logMerge = Log.LogMerge.newBuilder()
            .setFst(1L)
            .setSnd(2L)
            .setInfo("test")
            .build();

        Headers headers = new RecordHeaders();
        final byte[] serializedData = serializer.serialize("com.csx.testcmmn", headers, logMerge);

        Thread.sleep(5000L);

        //test still fails here
        DynamicMessage deserialize = deserializer.deserialize("com.csx.testcmmn", headers, serializedData);
        System.out.println("deserialize = " + deserialize);
    }

    static class ProtobufKafkaDeserializerWithHeaders extends ProtobufKafkaDeserializer {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(configs, isKey);
            if (Utils.isTrue(configs.get(USE_HEADERS))) {
                //noinspection unchecked
                headerUtils = new HeaderUtils((Map<String, Object>) configs, isKey);
            }
        }
    }

    static class ProtobufKafkaSerializerWithHeaders<U extends Message> extends ProtobufKafkaSerializer<U> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(configs, isKey);
            if (Utils.isTrue(configs.get(USE_HEADERS))) {
                //noinspection unchecked
                headerUtils = new HeaderUtils((Map<String, Object>) configs, isKey);
            }
        }
    }

}
