package org.apache.flink.contrib.streaming.state;

import java.net.UnknownHostException;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.Properties;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.contrib.streaming.state.utils.InetAddressLocalHostUtil;

import java.nio.charset.StandardCharsets;
import java.util.SplittableRandom;
import java.util.ConcurrentModificationException;

public class RedpandaConsumer<K, V, N> extends Thread{

    private final RedpandaKeyedStateBackend<K> backend;
    public Consumer<K, V> consumer;

    static String TOPIC;
    private final static String BOOTSTRAP_SERVERS = "localhost:9192"; //"192.168.122.131:9192";

    protected final DataOutputSerializer dataOutputView;
    protected final DataInputDeserializer dataInputView;   

    // /** Serializer for the state values. */
    SerializedCompositeKeyBuilder keyBuilder;

    RedpandaValueState<K, N, V> state;
    String key_class_name;
    String value_class_name;

    // For latency testing, keeping track of total latency over 100,000 records
    Double sample_rate = 0.05;;
    Integer num_records = (int) (10_000);
    Integer curr_records = 0;
    Integer warmup = 5;

    Long total_latency_from_produced = 0L;

    Boolean latency_printed = false;
    String hostAddress;
    SplittableRandom rand = new SplittableRandom();
    List<Long> latencies = new ArrayList<>();

    // for snapshotting
    Long latest_time = 0L;
    Long num_consumed = 0L;
    boolean in_control = true;    

    public RedpandaConsumer(
        RedpandaKeyedStateBackend<K> keyedBackend,
        RedpandaValueState<K, N, V> state_
    ) {
        backend = keyedBackend;

        // setup the data views
        this.dataOutputView = new DataOutputSerializer(128);
        this.dataInputView = new DataInputDeserializer();

        keyBuilder = new SerializedCompositeKeyBuilder<>(
            backend.getKeySerializer(),
            backend.getKeyGroupPrefixBytes(),
            32
        );

        state = (RedpandaValueState<K, N, V>) state_;

        TOPIC = state.TOPIC;
        System.out.println("Consuming from: " + TOPIC);
        key_class_name = state.key_class_name;
        value_class_name = state.value_class_name;

        // for latency testing
        try {
            this.hostAddress = InetAddressLocalHostUtil.getLocalHostAsString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        
    }

    private Consumer<K, V> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, state.directory_daemon_address+":9192");
        String tag = state.toString().substring(state.toString().lastIndexOf("@")+1);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "RPConsumer-"+tag);

        System.out.println("Consumer name: " + "RPConsumer-"+tag);

        // performance configs
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);
        // props.put("fetch.min.bytes", 100000000);
        props.put("max.poll.records", 250000);

        props.put("fetch.max.bytes", 52428800);
        props.put("max.partition.fetch.bytes", 52428800);

        // Create the consumer using props.
        if(key_class_name == "java.lang.String" && value_class_name == "java.lang.String"){
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName());
            consumer = (KafkaConsumer<K, V>) new KafkaConsumer<String, String>(props);
        }
        else if(key_class_name == "java.lang.String" && value_class_name == "java.lang.Long"){
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                        LongDeserializer.class.getName());
            consumer = (KafkaConsumer<K, V>) new KafkaConsumer<String, Long>(props);
        }
        else if(key_class_name == "java.lang.Long" && value_class_name == "java.lang.Long"){
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                        LongDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                        LongDeserializer.class.getName());
            consumer = (KafkaConsumer<K, V>) new KafkaConsumer<Long, Long>(props);
        }
        else{
            String error_message = String.format("Type combination %s and %s not supported yet.", key_class_name, value_class_name);
            throw new java.lang.UnsupportedOperationException(error_message);
        }

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private void makeUpdate(K key, V value){

        if (value == null) {
            // TODO: unimplemented
            // clear();
            return;
        }
        try {
            state.kvStore.put(key, value);
        } 
        catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    private void latencyTesting(ConsumerRecord<K, V> record){

        Boolean flag = false;

        for (Header header : record.headers()) {
            // System.out.println("LOOOK");
            // System.out.println(header.key());
            // System.out.println(this.hostAddress);
            if(header.key().equals("origin")){
                String origin_address = new String(header.value(), StandardCharsets.UTF_8);
                // System.out.println(origin_address);
                if(origin_address.equals(this.hostAddress)){
                    flag = true;
                }
            }
        }

        if(!flag){
            return;
        }

        // System.out.println(record);

        assert(this.latest_time <= record.timestamp());
        this.latest_time = record.timestamp();
        this.num_consumed += 1;

        // Latency testing: after this point, the new value is available in the user-code
        if(curr_records < num_records){
            long currentTime = System.currentTimeMillis();
            Long delta = currentTime - record.timestamp();

            total_latency_from_produced += delta;
            assert(delta > 0);
            curr_records += 1;

            latencies.add(delta);
        }

        if((curr_records % num_records == 0) && in_control){

            float mean = (float) total_latency_from_produced / curr_records;

            System.out.printf("[LATENCY]: %f\n", mean);

            float deviation = 0;
            for (Long latency : latencies) {
                deviation += Math.pow((latency-mean), 2);
            }
            double stdev = Math.sqrt(deviation/(curr_records-1));

            System.out.printf("[LATENCY_STDEV]: %f\n\n", stdev);
  
            curr_records = 0;
            total_latency_from_produced = 0L;
            latencies.clear();
        }
    }

    private void processRecord(ConsumerRecord<K, V> record){
        // System.out.printf("Processing Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
                // record.partition(), record.offset());

        try{
            K key = record.key();
            V value = record.value();
            
            this.makeUpdate(key, value);
            //this.makeUpdate((K) "test", value);

            // if(rand.nextDouble() < sample_rate){
            latencyTesting(record);
            // }
        }
        catch (Exception exception){
            System.out.println("Exception in processRecord(): " + exception);
        }
    }

    // call this synchronously before running the thread
    public void initialize() {
        if(this.consumer == null){
            this.consumer = createConsumer();
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        try {
            cl.loadClass("org.apache.kafka.clients.consumer.LogTruncationException");
            cl.loadClass("org.apache.kafka.common.message.OffsetForLeaderEpochResponseData$OffsetForLeaderTopicResultCollection");
            cl.loadClass("org.apache.kafka.common.message.OffsetForLeaderEpochRequestData$OffsetForLeaderTopicCollection");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$4");
            cl.loadClass("org.apache.kafka.common.message.ConsumerProtocolAssignment$TopicPartitionCollection");
            cl.loadClass("org.apache.kafka.common.message.ConsumerProtocolAssignment$TopicPartition");
            cl.loadClass("org.apache.kafka.clients.NetworkClient$1");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler$Builder");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler$FetchRequestData");

            cl.loadClass("org.apache.kafka.clients.consumer.OffsetAndMetadata");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$GroupSubscription");

            cl.loadClass("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$3");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$GroupAssignment");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$OffsetCommitCompletion");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.AbstractCoordinator$HeartbeatThread");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.AbstractCoordinator$HeartbeatThread$1");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor$MemberInfo");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$ListOffsetData");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$ListOffsetResult");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$1");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$7");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$FetchResponseMetricAggregator");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.Fetcher$FetchResponseMetricAggregator$FetchMetrics");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.RequestFuture$2");
            
            cl.loadClass("org.apache.kafka.common.requests.FetchMetadata");
            cl.loadClass("org.apache.kafka.common.requests.FetchRequest$PartitionData");
            cl.loadClass("org.apache.kafka.common.record.DefaultRecordBatch$3");
            cl.loadClass("org.apache.kafka.common.metrics.stats.Value");
            cl.loadClass("org.apache.kafka.common.network.Selector$CloseMode");
            cl.loadClass("org.apache.kafka.common.message.ConsumerProtocolAssignment");

            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerRecord");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerRecords$ConcatenatedIterable");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerRecords$ConcatenatedIterable$1");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$2");
            cl.loadClass("org.apache.kafka.clients.consumer.RetriableCommitFailedException");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$Assignment");

            cl.loadClass("net.openhft.chronicle.map.impl.CompiledMapQueryContext$1");
            cl.loadClass("net.openhft.chronicle.hash.impl.LocalLockState");
            cl.loadClass("net.openhft.chronicle.map.impl.CompiledMapQueryContext$SearchState");
            cl.loadClass("net.openhft.chronicle.map.impl.CompiledMapQueryContext$EntryPresence");
            cl.loadClass("net.openhft.chronicle.hash.impl.stage.entry.ChecksumHashing");
            cl.loadClass("net.openhft.chronicle.hash.impl.VanillaChronicleHash$TierBulkData");

        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // catch up to the target time
    public void catch_up(){
        Long poll_freq = 10L;

        System.out.println("CATCHING UP");
        this.in_control = false;

        while (true) {
            System.out.println(state.last_sent + " " + this.latest_time);
            try{
                final ConsumerRecords<K, V> consumerRecords = consumer.poll(poll_freq);
                if (consumerRecords.count() != 0) {
                    System.out.println("Num consumer records " + consumerRecords.count());
                    consumerRecords.forEach(record -> processRecord(record));
                    consumer.commitAsync();
                    System.out.println("Processed records");
                }
                else {
                    if(this.latest_time >= state.last_sent){
                        System.out.println("DONE CATCHING UP");
                        this.in_control = true;
                        return;
                    }
                }
            }
            catch (ConcurrentModificationException e){
                System.out.println("TODO[catch_up]: Beware concurrency...");
            }
        }
    }
    
    public void run() {

        Long last_time_consumed = System.currentTimeMillis();
        Long timeout = 5000L;
        Long poll_freq = 10L;

        while (true) {
            while(in_control){
                try{
                    // System.out.println("[REDPANDACONSUMER] About to poll!");
                    final ConsumerRecords<K, V> consumerRecords = consumer.poll(poll_freq);
                    // System.out.println("[REDPANDACONSUMER] I am polling!");
                    if (consumerRecords.count() != 0) {

                        // System.out.println("Num consumer records " + consumerRecords.count());
                        Long before_processing = System.currentTimeMillis();
                        consumerRecords.forEach(record -> processRecord(record));
                        Long after_processing = System.currentTimeMillis();
                        // System.out.println("Took [process]" + (after_processing-before_processing));
                        Long ms = after_processing-before_processing+1;
                        // System.out.println("put/ns: " + ((float)consumerRecords.count())/ms/1000 + " ("+ms+" ms), (n=" +consumerRecords.count()+")");
                        consumer.commitAsync();
                        //System.out.println("Took [commit] " + (System.currentTimeMillis()-after_processing));
                        // System.out.println("ChronicleMap size: "+ state.kvStore.size() +"\n");
                        // System.out.println("Off heap memory used: " + state.kvStore.offHeapMemoryUsed());

                        last_time_consumed = System.currentTimeMillis();

                        // System.out.println("Processed records");
                    }
                    else {
                        if(System.currentTimeMillis() - last_time_consumed > timeout){
                            try{
                                this.consumer.close();
                                state.producer.close();
                                state.kvStore.close();
                                

                                FileUtils.cleanDirectory(new File("/tmp/BackendChronicleMaps")); 
                            
                            }
                            catch (Exception e){
                                //
                                System.out.println(e);
                            }
                            
                            return;
                        }
                        //System.out.println(System.currentTimeMillis());
                    }
                }
                catch (ConcurrentModificationException e){
                    System.out.println("TODO[run]: Beware concurrency...");
                }
            }
        }
    }
}