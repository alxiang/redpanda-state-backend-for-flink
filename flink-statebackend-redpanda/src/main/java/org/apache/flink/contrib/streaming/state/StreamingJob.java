package org.apache.flink.contrib.streaming.state;
import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Redpanda imports
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer
import java.util.Properties;


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

    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static KafkaProducer<Long, String> createProducer() {
        // Properties gives properties to the KafkaProducer constructor, i.e. configuring the serialization
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "RedpandaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                    LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000); // lower timeout than default for debugging

        // TODO: temporary types
        return new KafkaProducer<Long, String>(props);
    }

    private static boolean writeMessage(String TOPIC, String value, KafkaProducer<Long, String> producer) {

        final ProducerRecord<Long, String> record =
                        new ProducerRecord<Long, String>(TOPIC, 0L, value);

        try {
            final RecordMetadata metadata = producer.send(record).get(); 
        }
        catch(Exception e) {
            return false;
        }
        finally {
            producer.flush();
            return true;
        }
    }

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env.fromElements(
			"To be, or not to be,--that is the question:--",
			"Whether 'tis nobler in the mind to suffer",
			"The slings and arrows of outrageous fortune",
			"Or to take arms against a sea of troubles,"
		);

		KafkaProducer<Long, String> producer = createProducer();

		for (long index = 0; index < 3; index++) {
			System.out.println(writeMessage("twitch_chat", "From main method " + index, producer));
		}
		producer.close();

		// we can send messages with kafka producer in the setup phase or we can send them with FlinkKafkaProducer

		stream.print();

		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
				
		stream.addSink(
			new FlinkKafkaProducer<String>(
				"twitch_chat",                  // target topic
				new ProducerStringSerializationSchema("twitch_chat"),    // serialization schema
				props,                  // producer config
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE // fault-tolerance);
			)
		); 

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}