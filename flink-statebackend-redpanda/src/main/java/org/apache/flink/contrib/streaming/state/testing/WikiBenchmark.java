package org.apache.flink.contrib.streaming.state.testing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.api.java.tuple.Tuple2;

// State backends
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

public class WikiBenchmark {

    public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();

		System.out.println("args: " + args[0]);
		System.out.println("args: " + args[1]);
		System.out.println("args: " + args[2]);

		if(args.length >= 1){
			if(args[0].equals("redpanda")){
				System.out.println("Using RedpandaStateBackend");
				env.setStateBackend(new RedpandaStateBackend());
			}
			else if(args[0].equals("hashmap")){
				System.out.println("Using HashMapStateBackend");
				env.setStateBackend(new HashMapStateBackend());
			}
			else if(args[0].equals("rocksdb")){
				System.out.println("Using RocksDBStateBackend");
				env.setStateBackend(new EmbeddedRocksDBStateBackend());
			}
		}

		String TOPIC = "Wiki";
		boolean async = false;

		if(args.length >= 2){
			if(args[1].equals("true")){
				async = true;
			}
		}
		if(args.length >= 3){
			TOPIC = args[2];
		}	

		// configure source
        DataStreamSource<String> source = env.readTextFile("file:///Users/alecxiang/flink-1.13.2/wikipedia/wiki-100k.txt").setParallelism(5);

        DataStream<Tuple2<String, Long>> tokenized = source.flatMap(new Tokenizer()).setParallelism(5);

		DataStream<Tuple2<String, Long>> mapper = tokenized.keyBy(record -> record.f0)
				.flatMap(new WordCountMap(TOPIC, async)) 
				.slotSharingGroup("map");

		mapper.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

		env.execute("Wikipedia Benchmark Job");
	}
    
}
