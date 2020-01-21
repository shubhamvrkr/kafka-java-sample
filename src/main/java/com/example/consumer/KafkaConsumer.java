package com.example.consumer;

import com.example.model.MessageData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka consumer process to consume message from broker and list partition details
 */
public class KafkaConsumer implements Runnable {

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, MessageData> consumer;
    private String topic;

    public KafkaConsumer(Properties properties, String topic){

        this.topic = topic;
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        this.consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {

        System.out.println("******************** Kafka Consumer Process Started ********************");

        //print partitions
        consumer.partitionsFor(topic).stream().forEach(partitionInfo -> System.out.println("Partition : "+partitionInfo.partition()));

        while (true) {

            //capture messages
            ConsumerRecords<String, MessageData> records = consumer.poll(Duration.of(5000, ChronoUnit.SECONDS));
            for (final ConsumerRecord<String, MessageData> record : records) {
                //process message
                System.out.println("Received message: " + record.value() + ", Partition: " + record.partition()+", Type: " +record.key() + ", Offset: " + record.offset());
            }
        }
    }
}
