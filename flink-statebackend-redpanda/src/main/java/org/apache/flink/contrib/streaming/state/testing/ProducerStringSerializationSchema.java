package org.apache.flink.contrib.streaming.state.testing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerStringSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Long>>{

    private String topic;   
    private LongSerializer longSerializer;
    private StringSerializer stringSerializer;

    public ProducerStringSerializationSchema(String topic) {
        super();
        this.topic = topic;
        
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element, Long timestamp) {

        longSerializer = new LongSerializer();
        stringSerializer = new StringSerializer();

        byte[] serializedTimestamp = longSerializer.serialize(this.topic, element.f1);
        byte[] serializedString = stringSerializer.serialize(this.topic, element.f0);
        
        // ProducerRecord(String topic, K key, V value)
        return new ProducerRecord<byte[], byte[]>(topic, serializedTimestamp, serializedString);
    }        
}