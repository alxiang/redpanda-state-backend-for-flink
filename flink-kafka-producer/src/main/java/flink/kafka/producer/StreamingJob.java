package flink.kafka.producer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;

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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink.kafka.producer-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c flink.kafka.producer.StreamingJob target/flink.kafka.producer-1.0-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		DataStream<String> stream = env.fromElements(
			"To be, or not to be,--that is the question:--",
			"Whether 'tis nobler in the mind to suffer",
			"The slings and arrows of outrageous fortune",
			"Or to take arms against a sea of troubles,"
		);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		
		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
				"twitch_chat",                  // target topic
				new SimpleStringSchema(),    // serialization schema
				properties,                  // producer config
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance
		
		stream.addSink(myProducer);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
