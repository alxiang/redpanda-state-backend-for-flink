package org.apache.flink.contrib.streaming.state;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class RedpandaConsumer<K> {
    private RedpandaKeyedStateBackend<K> backend;

    private final static String TOPIC = "word_chat";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public RedpandaConsumer(RedpandaKeyedStateBackend<K> keyedBackend) {
        this.backend = keyedBackend;
        this.runConsumer();
    }

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "RedpandaPollingConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private void runConsumer() {
        Thread thread = new Thread() {
            public void run() {
                final Consumer<Long, String> consumer = createConsumer();

                while (true) {
                    final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

                    System.out.println("Polled");

                    if (consumerRecords.count() != 0) {
                        consumerRecords.forEach(record -> {
                            System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
                                    record.partition(), record.offset());
                        });

                        consumer.commitAsync();
                    }
                }
            }
        };

        thread.start();
    }
}