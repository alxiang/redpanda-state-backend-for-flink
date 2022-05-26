package org.apache.flink.contrib.streaming.state.testing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

// State backends
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

public class WikiBenchmark {

    public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();

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

		// command line options
		String TOPIC = "Wiki";
		boolean async = false;
		String directory_daemon_address = "127.0.0.1";
		boolean redpanda = true;
		Long checkpointing_interval = 10L;

		if(args.length >= 2){
			if(args[1].equals("true")){
				async = true;
			}
		}
		if(args.length >= 3){
			TOPIC = args[2];
		}
		if(args.length >= 4){
			directory_daemon_address = args[3];
		}	
		if(args.length >= 5){
			if(args[4].equals("false")){
				redpanda = false;
			}
		}
		if(args.length >= 6){
			checkpointing_interval = Long.valueOf(args[5]);
		}

		String filePath = "file:///opt/flink/redpanda-state-backend-for-flink/wikipedia/wiki-100k.txt";

		DataStreamSource<String> source = env.readTextFile(filePath);

        DataStream<Tuple2<String, Long>> tokenized = source.flatMap(new Tokenizer());

		// System.out.println("[WIKI BENCHMARK] Set checkpointing interval to: " + checkpointing_interval);
		DataStream<Tuple2<String, Long>> mapper = tokenized.keyBy(record -> record.f0)
				.flatMap(new WordCountMap(TOPIC, async, directory_daemon_address, redpanda, checkpointing_interval)) 
				.slotSharingGroup("map");

		mapper.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

		env.execute("Wikipedia Benchmark Job");
	}
    
}
