package com.example.serializer;

import com.example.model.MessageData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Custom message data serializer to serialize the object to bytes
 */
public class MessageDataSerializer implements Serializer<MessageData> {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, MessageData data) {
        try{
            return mapper.writeValueAsString(data).getBytes();
        }catch (JsonProcessingException e){
            return null;
        }
    }
}
