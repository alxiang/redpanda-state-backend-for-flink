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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

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
    static String TOPIC = "Wiki";
    private final static String BOOTSTRAP_SERVERS = "localhost:9192"; //"192.168.122.131:9192";

    // Jiffy integration
    JiffyClient client;
    public String directory_daemon_address;

    private QueryEngine(String directory_daemon_address_){

        directory_daemon_address = directory_daemon_address_;

        // Create the consumer from Redpanda, subscribing to Wiki
        createConsumer();

        // Setup Jiffy connection
        connectToJiffy();

        // Ask Jiffy for a memory mapped file for the parquet file
        allocateJiffyFile("/usr/local/var/questdb/Table/default/column1.d");

        System.out.println("RedpandaConsumer, JiffyClient, and Jiffy files successfully initialized.");
    }

    private void createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, directory_daemon_address+":9192");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "QuestDBConsumer");

        // performance configs (borrowed from RedpandaConsumer)
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);
        props.put("max.poll.records", 250000);
        props.put("fetch.max.bytes", 52428800);
        props.put("max.partition.fetch.bytes", 52428800);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                    StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                    LongDeserializer.class.getName());
        consumer = (KafkaConsumer<String, Long>) new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
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

            client.createFile(filePath, "local://usr", hostAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SqlException {
        QueryEngine myEngine = new QueryEngine("192.168.122.132");

        final CairoConfiguration configuration = new DefaultCairoConfiguration("/tmp/questdb");
        // CairoEngine is a resource manager for embedded QuestDB
        try (CairoEngine engine = new CairoEngine(configuration)) { 
            // Execution context is a conduit for passing SQL execution artefacts to the execution site
            final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
            try (SqlCompiler compiler = new SqlCompiler(engine)) {

                // PageFrameCursor cursor = ...; // Setup PageFrameCursor instance
                // An easy way to create the table
                compiler.compile("create table abc (word string, count long, ts timestamp) timestamp(ts)", ctx);

                // This TableWriter instance has an exclusive (intra and interprocess) lock on the table
                try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), "abc", "testing")) {
                    for (int i = 0; i < 11; i++){
                        TableWriter.Row row = writer.newRow();//Os.currentTimeMicros());
                        row.putStr(0, "hello");
                        row.putLong(1, i);
                        row.append();
                    }
                    writer.commit();
                }
            }
        }
    }
}
