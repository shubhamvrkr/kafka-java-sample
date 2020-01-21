package com.example.producer;

import com.example.model.MessageData;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Producer class to create topic, partition and publish messages to kafka broker
 */
public class KafkaProducer {

    private Properties properties;
    private Producer<String, MessageData> producer;
    private String topic;

    public KafkaProducer(Properties properties, String topic){
        this.properties = properties;
        this.topic = topic;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.properties);
    }

    //creates new topic with partitions and replication in kafka
    public void createTopic(int partitions, int replication){


        final NewTopic newTopic = new NewTopic(this.topic, partitions, (short) replication);
        AdminClient client = AdminClient.create(properties);
        CreateTopicsResult result = client.createTopics(Collections.singleton((newTopic)));
        try{
            result.values().get(topic).get();
            System.out.println("Topic created successfully!!");
        }catch (Exception e){
            System.err.println("Failed creating topic due to: "+e.getMessage());
            e.printStackTrace();
        }
    }


    //creates new partition in the topic
    public void addPartitions(int count){

        AdminClient client = AdminClient.create(properties);
        Map<String, NewPartitions> counts = new HashMap<>();
        counts.put(topic, NewPartitions.increaseTo(count));
        CreatePartitionsResult results = client.createPartitions(counts);
        try{
            results.values().get(topic).get();
            System.out.println("Created partitions successfully!!");
        }catch (Exception e){
            System.err.println("Failed creating partitions due to: "+e.getMessage());
        }
    }

    //sends message to kafka broker
    public RecordMetadata send(MessageData messageData){

        Future<RecordMetadata> future = this.producer.send(new ProducerRecord<>(topic, messageData.getType(), messageData));
        try{
            return future.get();
        }catch (InterruptedException | ExecutionException e){
            System.err.println("Failed publishing data to kafka broker due to: "+e.getMessage());
        }
        return null;
    }

    //closes producer
    public void close(){

        this.producer.close();
    }
}
