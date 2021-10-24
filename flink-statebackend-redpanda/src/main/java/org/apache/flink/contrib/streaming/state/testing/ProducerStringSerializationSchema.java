package org.apache.flink.contrib.streaming.state.testing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class ProducerStringSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Long>>{

    private String topic;   

    public ProducerStringSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element, Long timestamp) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(element.f1);
        
        // ProducerRecord(String topic, K key, V value)
        return new ProducerRecord<byte[], byte[]>(topic, element.f0.getBytes(StandardCharsets.UTF_8));
    }        
}