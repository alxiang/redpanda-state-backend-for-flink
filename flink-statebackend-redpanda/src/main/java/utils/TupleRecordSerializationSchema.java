package utils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

public class TupleRecordSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Long>> {
    StringSerializer string_serializer;
    LongSerializer long_serializer;
    String topic;

    public TupleRecordSerializationSchema(String topic_){
        topic = topic_;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element, Long timestamp) {
        string_serializer = new StringSerializer();
        long_serializer = new LongSerializer();

        byte[] key = string_serializer.serialize(topic, element.f0);
        byte[] val = long_serializer.serialize(topic, element.f1);

        return new ProducerRecord<byte[],byte[]>(topic, key, val);
    }
}
    