package org.apache.flink.contrib.streaming.state.testing;
import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.api.java.tuple.Tuple2;

// Redpanda imports
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;
import org.apache.flink.contrib.streaming.state.benchmark.WordCount;
import org.apache.flink.contrib.streaming.state.benchmark.WordSource;
import static org.apache.flink.contrib.streaming.state.benchmark.JobConfig.WORD_LENGTH;
import static org.apache.flink.contrib.streaming.state.benchmark.JobConfig.WORD_NUMBER;
import static org.apache.flink.contrib.streaming.state.benchmark.JobConfig.WORD_RATE;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class PrintingJobBenchmark {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new RedpandaStateBackend());
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();

		// configure source
		DataStream<Tuple2<String, Long>> source = 
		WordSource.getSource(env, WORD_RATE.defaultValue() , WORD_NUMBER.defaultValue(), WORD_LENGTH.defaultValue())
					.slotSharingGroup("src");

		DataStream<Tuple2<String, Long>> mapper = source.keyBy(record -> 0L)
				.flatMap(new WordCountMap()) 
				.slotSharingGroup("map");

		mapper.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

		env.execute("Flink Printing Job Benchmark (Redpanda)");
	}
}

