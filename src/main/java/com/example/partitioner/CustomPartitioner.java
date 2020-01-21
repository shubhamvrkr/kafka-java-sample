package com.example.partitioner;

import com.example.Main;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Custom partitioner class that decides partiton index based on message type (i.e {@link com.example.Main.ObjectTypes})
 */
public class CustomPartitioner implements Partitioner {

    private Main.ObjectTypes[] types;

    public CustomPartitioner() {

        types = Main.ObjectTypes.values();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        String type = (String) key;
        int index = Main.ObjectTypes.valueOf(type).ordinal();
        return index;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
