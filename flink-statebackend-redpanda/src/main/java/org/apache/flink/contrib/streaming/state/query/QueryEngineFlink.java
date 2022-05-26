package org.apache.flink.contrib.streaming.state.query;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

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

        String table_name = "wikitable" + Math.random();

        QueryFlinkKafkaConsumer<KafkaRecord> consumer = 
            new QueryFlinkKafkaConsumer<KafkaRecord>
                ("Wiki", 
                new KafkaRecordSchema(),
                props
            );

		DataStream<KafkaRecord> source = env
            .addSource(consumer)
            .name("Kafka Source")
            .uid("Kafka Source");

		source.flatMap(new QuestDBInsertMap(table_name, directory_daemon_address))
            .addSink(new DiscardingSink<>())
			.slotSharingGroup("sink");

		env.execute("Query Engine Flink Job");
	}
}