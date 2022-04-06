package org.apache.flink.contrib.streaming.state.query;

// Jiffy Client imports
import jiffy.JiffyClient;
import org.apache.flink.contrib.streaming.state.utils.InetAddressLocalHostUtil;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

// Redpanda consumer imports
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer

// QuestDB imports
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;

import java.net.UnknownHostException;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.Properties;


public class QueryEngine {

    // Redpanda integration
    public Consumer<String, Long> consumer;
    public Producer<String, Long> producer;
    public Consumer<String, Long> checkpoint_consumer;

    static String TOPIC = "Wiki";
    private final static String BOOTSTRAP_SERVERS = "localhost:9192"; //"192.168.122.131:9192";
    public Long checkpoint_offset = 0L; // the offset sent from RedpandaValueState during a checkpoint
    public Long latest_offset;
    public Long latest_ts = 0L;
    public Long latest_committed_ts = 0L;
    public Long first_ts;

    // Jiffy integration
    JiffyClient client;
    public String directory_daemon_address;

    private QueryEngine(String table_name, String directory_daemon_address_){

        directory_daemon_address = directory_daemon_address_;

        // Create the consumer from Redpanda, subscribing to Wiki
        createConsumer();

        // Create consumer subscribing to WikiCheckpoint
        createCheckpointConsumer();

        // Create a producer publishing consumed offsets to Redpanda at WikiOffsets
        createProducer();

        // Setup Jiffy connection
        connectToJiffy();

        // Ask Jiffy for a memory mapped file for the parquet file
        allocateJiffyFile("/home/alec/.questdb/"+table_name+"/default/count.d");
        allocateJiffyFile("/home/alec/.questdb/"+table_name+"/default/ts.d");
        allocateJiffyFile("/home/alec/.questdb/"+table_name+"/default/word.d");
        allocateJiffyFile("/home/alec/.questdb/"+table_name+"/default/word.i");

        System.out.println("RedpandaConsumer, JiffyClient, and Jiffy files successfully initialized.");
    }

    private void createProducer() {
        // Configuring the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            directory_daemon_address+":9192");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "OffsetProducer (QueryEngine)");

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
    
    public void produceOffset() {
        final ProducerRecord<String, Long> record;

        record = new ProducerRecord<String, Long>(
            TOPIC+"Offsets", 
            "placeholder",
            latest_offset
        );
        
        this.producer.send(record);
    }


    private void createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, directory_daemon_address+":9192");
        String tag = this.toString().substring(this.toString().lastIndexOf("@")+1);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "QuestDBConsumer-"+tag);

        // performance configs (borrowed from RedpandaConsumer)
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);
        props.put("max.poll.records", 250000);
        props.put("fetch.max.bytes", 52428800);
        props.put("max.partition.fetch.bytes", 52428800);
        props.put("auto.offset.reset", "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                    StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                    LongDeserializer.class.getName());
        consumer = (KafkaConsumer<String, Long>) new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
    }

    private void createCheckpointConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, directory_daemon_address+":9192");
        String tag = this.toString().substring(this.toString().lastIndexOf("@")+1);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "QuestDBConsumer-"+tag+"checkpoint");

        // performance configs (borrowed from RedpandaConsumer)
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);
        props.put("max.poll.records", 250000);
        props.put("fetch.max.bytes", 52428800);
        props.put("max.partition.fetch.bytes", 52428800);
        props.put("auto.offset.reset", "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                    StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                    LongDeserializer.class.getName());
        checkpoint_consumer = (KafkaConsumer<String, Long>) new KafkaConsumer<String, Long>(props);
        checkpoint_consumer.subscribe(Collections.singletonList(TOPIC+"Checkpoint"));
    }

    private void connectToJiffy() {

        try {
            System.out.println("Trying to connect to Jiffy at address: " + directory_daemon_address);
            this.client = new JiffyClient(directory_daemon_address, 9090, 9091);
        } catch (Exception e) {
            System.out.println("Failed to connect to Jiffy with client, are the Jiffy directory and storage daemons running?");
            System.out.println(e);
            System.exit(-1);
        }
    }

    private void allocateJiffyFile(String filePath) {
        try {
            String hostAddress = InetAddressLocalHostUtil.getLocalHostAsString();
            System.out.println("Got the local host address as: " + hostAddress);
            System.out.println("Asking for Jiffy file at: " + filePath);

            client.createFile(filePath, "local://home", hostAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processRecord(ConsumerRecord<String, Long> record, TableWriter writer) {

        if(first_ts == null){
            first_ts = record.timestamp();
        }

        String key = record.key();
        Long value = record.value();
        latest_offset = record.offset();
        latest_ts = record.timestamp();

        TableWriter.Row row = writer.newRow(record.timestamp());
        row.putStr(0, key);
        row.putLong(1, value);
        row.append();
    }

    private void processCheckpointRecord(ConsumerRecord<String, Long> record){
        // System.out.println(record.value());
        // System.out.println(checkpoint_offset);
        if(record.value() != null){
            if(checkpoint_offset < record.value()){
                checkpoint_offset = record.value();
            }
        }
    }

    private void commitOperation(){
        
    }

    public static void main(String[] args) throws SqlException {

        Long timeout = 1800000L;
        Long poll_freq = 10L;
        Long checkpointing_interval = 10L;
        if(args.length >= 1){
            checkpointing_interval = Long.valueOf(args[0]);
        }
        System.out.println("Checkpointing interval (commit frequency): " + checkpointing_interval);
        

        String table_name = "wikitable";
        QueryEngine redpanda_engine = new QueryEngine(table_name, "192.168.122.132");

        final CairoConfiguration configuration = new DefaultCairoConfiguration("/home/alec/.questdb");
        // CairoEngine is a resource manager for embedded QuestDB
        try (CairoEngine engine = new CairoEngine(configuration)) { 
            // Execution context is a conduit for passing SQL execution artefacts to the execution site
            final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
            try (SqlCompiler compiler = new SqlCompiler(engine)) {

                // drop the table if it exists
                try {
                    compiler.compile("drop table "+table_name, ctx);
                } catch (Exception e) {
                    //TODO: handle exception
                }
                
                // An easy way to create the table
                compiler.compile("create table "+table_name+" (word string, count long, ts timestamp) timestamp(ts)", ctx);

                // This TableWriter instance has an exclusive (intra and interprocess) lock on the table
                try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), table_name, "testing")) {
                    
                    System.out.println("Ready to consume from Redpanda");
                    Long last_time_consumed = System.currentTimeMillis();
                    while (true) {

                        // Update checkpoint_offset if necessary
                        ConsumerRecords<String, Long> checkpoint_offsets = redpanda_engine.checkpoint_consumer.poll(0L);
                        if (checkpoint_offsets.count() != 0) {
                            checkpoint_offsets.forEach(record -> redpanda_engine.processCheckpointRecord(record));
                        }

                        final ConsumerRecords<String, Long> consumerRecords = redpanda_engine.consumer.poll(poll_freq);
                       
                        if (consumerRecords.count() != 0) {
                            System.out.println("Received records: " + consumerRecords.count());
                            
                            
                            consumerRecords.forEach(record -> redpanda_engine.processRecord(record, writer));
                            System.out.println("Runtime: " + (redpanda_engine.latest_ts - redpanda_engine.first_ts));
                            if(redpanda_engine.latest_offset >= redpanda_engine.checkpoint_offset){
                                System.out.println("Committing with: " + redpanda_engine.latest_offset);
                                writer.commit();
                                redpanda_engine.latest_committed_ts = redpanda_engine.latest_ts;

                                redpanda_engine.consumer.commitAsync();
                                redpanda_engine.produceOffset();
                                last_time_consumed = System.currentTimeMillis();
                            }                            
                        }
                        else {
                            if(System.currentTimeMillis() - last_time_consumed > timeout){
                                System.out.println("Timing out and exiting gracefully");
                                try{
                                    redpanda_engine.consumer.close();                                
                                }
                                catch (Exception e){
                                    System.out.println(e);
                                }
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}