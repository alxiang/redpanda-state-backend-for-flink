package org.apache.flink.contrib.streaming.state.query;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class QueryEngineFlink {
      
    public static void main(String[] args) throws Exception {

        String directory_daemon_address = "127.0.0.1";

        if(args.length >= 1){
			directory_daemon_address = args[0];
		}	

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();
		env.enableCheckpointing(1000);

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.122.132:9192");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "QuestDBConsumerFlink");
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);
        props.put("max.poll.records", 250000);
        props.put("fetch.max.bytes", 52428800);
        props.put("max.partition.fetch.bytes", 52428800);
        props.put("auto.offset.reset", "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                    StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                    LongDeserializer.class.getName());

        String table_name = "wikitable";

        HashMap<TopicPartition, Long> partition_map = new HashMap<TopicPartition,Long>();
        partition_map.put(
            new TopicPartition("Wiki", 0), 
            500L
        );

        KafkaSource<KafkaRecord> source = KafkaSource
            .<KafkaRecord>builder()
            .setBootstrapServers("192.168.122.132:9192")
            .setGroupId("QuestDBConsumerFlink")
            .setTopics("Wiki")
            .setDeserializer(new KafkaRecordSchema())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.offsets(partition_map))
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .flatMap(new QuestDBInsertMap(table_name, directory_daemon_address))
            .addSink(new DiscardingSink<>())
			.slotSharingGroup("sink");

		env.execute("Query Engine Flink Job");
	}
}
