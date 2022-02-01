package org.apache.flink.contrib.streaming.state;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.StateSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.state.State;

public class RedpandaConsumer<K, V, N> extends Thread{

    private final RedpandaKeyedStateBackend<K> backend;
    private Consumer<K, V> consumer;

    // TODO(ALEC): grab the programmatically generated topic name from ValueState
    static String TOPIC;
    private final static String BOOTSTRAP_SERVERS = "localhost:9192";

    protected final DataOutputSerializer dataOutputView;
    protected final DataInputDeserializer dataInputView;   

    // /** Serializer for the state values. */
    private TypeSerializer<V> valueSerializer;
    private TypeSerializer<K> keySerializer;

    private final SerializedCompositeKeyBuilder<K> sharedKeyBuilder;
    SerializedCompositeKeyBuilder keyBuilder;

    // configured!
    final String stateName;
    RedpandaValueState<K, N, V> state;
    String key_class_name;
    String value_class_name;

    // For latency testing, keeping track of total latency over 100,000 records
    Integer num_records = 100; //100_000;
    Integer curr_records = 0;
    Integer warmup = 25;

    Long total_latency_from_produced = 0L;

    Boolean latency_printed = false;

    public RedpandaConsumer(
        RedpandaKeyedStateBackend<K> keyedBackend,
        RedpandaValueState<K, N, V> state_
    ) {
        backend = keyedBackend;

        // setup the data views
        this.dataOutputView = new DataOutputSerializer(128);
        this.dataInputView = new DataInputDeserializer();

        sharedKeyBuilder = backend.getSharedKeyBuilder();
        keyBuilder = new SerializedCompositeKeyBuilder<>(
            backend.getKeySerializer(),
            backend.getKeyGroupPrefixBytes(),
            32
        );

        // For PrintingJob, this can be found in WordCountMap.open() and is 'Word counter'
        stateName = "Word counter"; 
        state = (RedpandaValueState<K, N, V>) state_;

        TOPIC = state.TOPIC;
        key_class_name = state.key_class_name;
        value_class_name = state.value_class_name;
    }

    private Consumer<K, V> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "RPConsumer-3.0");

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

        // keyBuilder.setKeyAndKeyGroup(key, 0);
        if (value == null) {
            // TODO: unimplemented
            // clear();
            return;
        }
        try {
            // System.out.println("[REDPANDACONSUMER]kvStore put");
            state.kvStore.put(key, value);
        } 
        catch (java.lang.Exception e) {
            System.out.println(e);
            System.out.println(key);
            System.out.println(value);
            System.out.println(state.kvStore);
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    private void processRecord(ConsumerRecord<K, V> record){
        // System.out.printf("Processing Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
        //         record.partition(), record.offset());

        // TODO: like for the producer, we would ideally template type deciding so logic is not in hot path

        try{
            K key = record.key();
            V value = record.value();
            
            // TODO: keys must be of type long -- find dynamic way to do this
            this.makeUpdate(key, value);
            
            // Latency testing: after this point, the new value is available in the user-code
            if(curr_records < num_records){
                long currentTime = System.currentTimeMillis();

                total_latency_from_produced += (currentTime - record.timestamp());
                assert(currentTime - record.timestamp() > 0);
                
                curr_records += 1;
            }

            if((curr_records % num_records == 0)){
                if(warmup > 0){
                    System.out.println("===LATENCY TESTING RESULTS [WARMUP]===");
                    warmup -= 1;
                }
                else{
                    System.out.println("===LATENCY TESTING RESULTS===");
                }

                System.out.printf("Average Latency (from Producer): %f\n\n", 
                    (float) total_latency_from_produced / curr_records);
      
                curr_records = 0;
                total_latency_from_produced = 0L;
            }
            // System.out.printf("updated state for %s to %d\n", word_key, state.value());
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
            cl.loadClass("org.apache.kafka.clients.NetworkClient$1");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler$Builder");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler$FetchRequestData");

            cl.loadClass("org.apache.kafka.clients.consumer.OffsetAndMetadata");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$GroupSubscription");

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

        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    // this can be called asynchronously, i.e. with thread.start(), which does not block the main thread
    // or it can be called synchronousy, i.e. with thread.run(), which does block the main thread
    public void run() {

        keySerializer = (TypeSerializer<K>) state.keySerializer;
        valueSerializer = (TypeSerializer<V>) state.valueSerializer;

        while (true) {
            // System.out.println("[REDPANDACONSUMER] About to poll!");
            final ConsumerRecords<K, V> consumerRecords = consumer.poll(10L);
            // System.out.println("[REDPANDACONSUMER] I am polling!");
            if (consumerRecords.count() != 0) {

                // System.out.println("Num consumer records " + consumerRecords.count());

                consumerRecords.forEach(record -> processRecord(record));
                consumer.commitAsync();

                // System.out.println("Processed records");
            }
            else {
                // System.out.println("No records.");
            }
        }
    }
}