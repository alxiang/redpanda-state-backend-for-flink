package org.apache.flink.contrib.streaming.state.query;

// Redpanda
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongSerializer; // record key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer
import java.lang.InterruptedException;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class CheckpointTimer {


    static String TOPIC = "Wiki";
    private final static String BOOTSTRAP_SERVERS = "localhost:9192";
    String directory_daemon_address;
    public Producer<String, Long> producer;
    public Long last_checkpoint = 0L;

    private CheckpointTimer(String directory_daemon_address_){

        directory_daemon_address = directory_daemon_address_;

        createProducer();

    }

    private void createProducer() {
        // Configuring the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            directory_daemon_address+":9192");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "CheckpointTimer");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());

        // for improving synchronous writing
        props.put("acks", "1"); // acknowledgement only from leader broker
        props.put("max.in.flight.requests.per.connection", "1"); // ordering guarantees

        // Handle dynamic types, though String may be enough for query engines (convert strings to json)
        producer = new KafkaProducer<String, Long>(props);
    }

    private void produceCheckpoint(){
        final ProducerRecord<String, Long> record;

        record = new ProducerRecord<String, Long>(
            TOPIC, 
            "$FLINKCHECKPOINT",
            0L
        );
        
        this.producer.send(record);

        Long now = System.currentTimeMillis();
        System.out.println(now - last_checkpoint);
        last_checkpoint = now;
    }

    public static void main(String[] args) {
        
        CheckpointTimer checkpoint_timer = new CheckpointTimer("192.168.122.132");
        Long interval = 1000L;
        if(args.length >= 1){
            interval = Long.valueOf(args[0]);
        }

        Runnable task = () -> checkpoint_timer.produceCheckpoint();

        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(task, 0L, interval, TimeUnit.MILLISECONDS);
    }
}
