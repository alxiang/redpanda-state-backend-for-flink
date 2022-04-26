package org.apache.flink.contrib.streaming.state.query;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

public class KafkaRecordSchema implements KafkaDeserializationSchema<KafkaRecord> {
    public KafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) {
        KafkaRecord rec = new KafkaRecord();
        StringDeserializer string_deserializer = new StringDeserializer();
        LongDeserializer long_deserializer = new LongDeserializer();

        rec.key = string_deserializer.deserialize(record.topic(), record.key());
        rec.value = long_deserializer.deserialize(record.topic(), record.value());
        rec.topic = record.topic();
        rec.offset = record.offset();
        rec.timestamp = record.timestamp();
    //   ...

        return rec; 
    }   
    public boolean isEndOfStream(KafkaRecord nextElement){
        return false;
    }
    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
    