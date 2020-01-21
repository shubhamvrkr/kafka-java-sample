package com.example.serializer;

import com.example.model.MessageData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Custom message data de-serializer to deserialize build object from bytes
 */
public class MessageDataDeserializer implements Deserializer<MessageData> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public MessageData deserialize(String topic, byte[] data) {

        MessageData msgData = null;
        try {
            msgData = mapper.readValue(data, MessageData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return msgData;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
