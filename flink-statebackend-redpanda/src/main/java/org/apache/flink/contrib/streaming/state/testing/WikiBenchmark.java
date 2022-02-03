package org.apache.flink.contrib.streaming.state.testing;
import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.api.java.tuple.Tuple2;

// Redpanda imports
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;
import org.apache.flink.runtime.metrics.util.SystemResourcesCounter;



public class WikiBenchmark {

    public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new RedpandaStateBackend());
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();

		// configure source
        DataStreamSource<String> source = env.readTextFile("file:///Users/alecxiang/flink-1.13.2/redpanda-state-backend-for-flink");
		// DataStream<Tuple2<String, Long>> source = 
		// WordSource.getSource(env, WORD_RATE.defaultValue(), WORD_NUMBER.defaultValue(), WORD_LENGTH.defaultValue())
		// 			.slotSharingGroup("src");

        DataStream<Tuple2<String, Long>> tokenized = source.flatMap(new Tokenizer());

		DataStream<Tuple2<String, Long>> mapper = tokenized.keyBy(record -> record.f0)
				.flatMap(new WordCountMap()) 
				.slotSharingGroup("map");

		mapper.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

		env.execute("Wikipedia Benchmark Job");
	}
    
}
