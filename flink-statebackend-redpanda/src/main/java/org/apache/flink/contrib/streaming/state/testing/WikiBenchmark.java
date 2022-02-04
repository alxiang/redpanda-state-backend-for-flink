package org.apache.flink.contrib.streaming.state.testing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.api.java.tuple.Tuple2;

// State backends
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

public class WikiBenchmark {

    public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStateBackend(new RedpandaStateBackend());
		env.setStateBackend(new HashMapStateBackend());
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();

		// configure source
        DataStreamSource<String> source = env.readTextFile("file:///Users/alecxiang/flink-1.13.2/wikipedia/wiki-1k.txt").setParallelism(5);

        DataStream<Tuple2<String, Long>> tokenized = source.flatMap(new Tokenizer()).setParallelism(5);

		DataStream<Tuple2<String, Long>> mapper = tokenized.keyBy(record -> record.f0)
				.flatMap(new WordCountMap("Wiki")) 
				.slotSharingGroup("map");

		mapper.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

		env.execute("Wikipedia Benchmark Job");
	}
    
}
