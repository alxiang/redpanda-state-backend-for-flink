// tutorial: http://cloudurable.com/blog/kafka-tutorial-kafka-producer/index.html

// Notes:
// Kafka producer needs a list of bootstrap servers or Kafka brokers
// Message id will be encoded as Kafka record key, message body as Kafka record value
// -> this means we need to specify a key and value serializer

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer
import java.util.Properties;

public class RedpandaProducerExample {

    // I set up a Redpanda cluster as specified here: https://vectorized.io/docs/quick-start-docker/#Do-some-streaming
    // I am publishing a "twitch_chat" topic with localhost:9092 as one of the brokers
    private final static String TOPIC = "twitch_chat";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Producer<Long, String> createProducer() {
        // Properties gives properties to the KafkaProducer constructor, i.e. configuring the serialization
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "RedpandaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
  
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, index,
                                    "Hello world " + index);
  
                RecordMetadata metadata = producer.send(record).get(); // returns a java future
  
                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);
  
            }
        } finally {
            producer.flush(); // flushes any accumulated records, though Kafka will auto flush
            producer.close(); // this is 'polite'
        }
    }
    
    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer(5);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }
    
}
