package utils;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

public class KafkaRecordSchema implements KafkaRecordDeserializationSchema<KafkaRecord> {
    StringDeserializer string_deserializer;
    LongDeserializer long_deserializer;

    public boolean isEndOfStream(KafkaRecord nextElement){
        return false;
    }
    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord> out) throws IOException {
        KafkaRecord rec = new KafkaRecord();
        string_deserializer = new StringDeserializer();
        long_deserializer = new LongDeserializer();

        rec.key = string_deserializer.deserialize(record.topic(), record.key());
        rec.value = long_deserializer.deserialize(record.topic(), record.value());
        rec.topic = record.topic();
        rec.offset = record.offset();
        rec.timestamp = record.timestamp();

        out.collect(rec);
    }
}
    