package com.example;

import com.example.consumer.KafkaConsumer;
import com.example.model.MessageData;
import com.example.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class Main {

    //topic to be created in kafka
    private static final String TOPIC = "TEST";

    public static void main(String[]  args){

        //get configurations
        Properties properties = getKafkaProperties();

        //initiate kafka producer
        KafkaProducer kafkaProducer = new KafkaProducer(properties,TOPIC);

        //create topic with initial partition count
        kafkaProducer.createTopic(ObjectTypes.values().length,1);

        //increase partition count to a total of 11 (i.e 5 new partitions created )
        kafkaProducer.addPartitions(11);


        //start consumer process
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties,TOPIC);
        Thread consumerThread = new Thread(kafkaConsumer);
        consumerThread.start();

        //play some 20 messages
        for(int i=0;i<20;i++){
            MessageData messageData = getSampleMessage(i%ObjectTypes.values().length);
            RecordMetadata recordMetadata = kafkaProducer.send(messageData);
            if(recordMetadata!=null){
                System.out.println("Published message: " + messageData + ", Partition: " + recordMetadata.partition()+", Type: " +messageData.getType());
            }
        }

        kafkaProducer.close();

    }

    //configuration for kafka broker
    private static Properties getKafkaProperties(){

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("retries", 0);
        props.put("acks", "all");
        props.put("group.id", "1");
        //max batching of request
        props.put("batch.size", 16384);
        //batching timeout
        props.put("linger.ms", 2);
        props.put("enable.auto.commit", "true");
        props.put("buffer.memory", 33554432);
        //custom serializer/deserializer for key and values
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "com.example.serializer.MessageDataSerializer");
        props.put("partitioner.class", "com.example.partitioner.CustomPartitioner");
        props.put("value.deserializer", "com.example.serializer.MessageDataDeserializer");

        return props;
    }

    //random data generator
    private static MessageData getSampleMessage(int index){

        return new MessageData(UUID.randomUUID().toString(),ObjectTypes.values()[index].name(),
                Arrays.asList(UUID.randomUUID().toString(),UUID.randomUUID().toString()));
    }

    //each object type should go in seperate partition
    //new object types can be added later, hence partition should be increased dynamically
    public enum ObjectTypes{

        MANGO,
        BANANA,
        KIWI,
        PAPAYA,
        WATERMELON,
        GUAVA
    }
}
