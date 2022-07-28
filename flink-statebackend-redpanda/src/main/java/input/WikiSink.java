package input;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.LongSerializer; 
import org.apache.kafka.common.serialization.StringSerializer;

import utils.Tokenizer;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

// State backends
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import utils.KafkaRecord;
import utils.TupleRecordSerializationSchema;

public class WikiSink {

    public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();
        env.setStateBackend(new HashMapStateBackend());

		// command line options
		String TOPIC = "Wiki";
		String directory_daemon_address = "127.0.0.1";

		if(args.length >= 1){
			directory_daemon_address = args[0];
		}	

		String filePath = "file:///opt/flink/redpanda-state-backend-for-flink/wikipedia/wiki-100k.txt";

		DataStreamSource<String> source = env.readTextFile(filePath);

        DataStream<Tuple2<String, Long>> tokenized = source.flatMap(new Tokenizer());

		DataStream<Tuple2<String, Long>> mapper = tokenized.keyBy(record -> record.f0)
				.flatMap(new WordCountMap()); 

		
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            directory_daemon_address+":9192");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "FlinkWikiProducer");


		mapper.addSink(
			new FlinkKafkaProducer<>(
				TOPIC,
				new TupleRecordSerializationSchema(TOPIC),
				props,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE
			)
		);

		env.execute("Wikipedia Producer Job");
	}
}
