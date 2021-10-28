package org.apache.flink.contrib.streaming.state.testing;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RedpandaStateBackend;

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

public class PrintingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new RedpandaStateBackend());
		env.getConfig().setMaxParallelism(1);

        // read an infinite stream
        env.fromElements(
			new Tuple2<String, Long>("apple", 0L),
			new Tuple2<String, Long>("banana", 0L),
			new Tuple2<String, Long>("cherry", 0L),
			new Tuple2<String, Long>("dragonfruit", 0L),
			new Tuple2<String, Long>("elderberry", 0L),
			new Tuple2<String, Long>("apple", 0L)
		).keyBy(record -> record.f0)
        .flatMap(new WordCountMap()) // while reading the stream, employ ValueState which is updated by our backend
        .print();

		// execute program
        env.execute("Flink Printing Job from Redpanda");
	}
}
