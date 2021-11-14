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

/*
Stuff to print in ValueState (configurations)
- this.backend.stateToStateName.get(this) -> stateName
- currentNamespace and the stateName
- keySerializer and valueSerializer
*/
public class RedpandaConsumer<K, V, N> extends Thread{

    private final RedpandaKeyedStateBackend<K> backend;
    private Consumer<String, String> consumer;

    private final static String TOPIC = "word_chat";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    protected final DataOutputSerializer dataOutputView;
    protected final DataInputDeserializer dataInputView;   

    // /** Serializer for the namespace. */
    // final TypeSerializer<N> namespaceSerializer;
    // /** Serializer for the state values. */
    private TypeSerializer<V> valueSerializer;

    private TypeSerializer<K> keySerializer;

    private final SerializedCompositeKeyBuilder<K> sharedKeyBuilder;
    SerializedCompositeKeyBuilder keyBuilder;

    // configured!
    final String stateName;
    // final N currentNamespace;
    RedpandaValueState<K, N, Long> state;
    
    

    // For latency testing:
    // keep track of total latency over 100,000,000 records 
    Integer num_records = 1000000;
    Integer curr_records = 0;

    // currentTimeMillis - record.timestamp()
    Long total_latency_from_produced = 0L;
    // currentTimeMillis - record.key()
    Long total_latency_from_source = 0L;

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
        state = (RedpandaValueState<K, N, Long>) state_;

        // For PrintingJob, the namespace is VoidNamespace.
        // We can tell this by printing the namespace in ValueState.value()
        // ex: https://www.programcreek.com/java-api-examples/?api=org.apache.flink.runtime.state.VoidNamespace
        // currentNamespace = (N) VoidNamespace.INSTANCE;
        // namespaceSerializer = (TypeSerializer<N>) VoidNamespaceSerializer.INSTANCE;
    }

    private static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "RedpandaPollingConsumer");
        // props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // props.put("session.timeout.ms", 30000);
        // props.put("max.poll.interval.ms", 43200000);
        // props.put("request.timeout.ms", 43205000);

        // performance configs
        // props.put("fetch.min.bytes", 100000000);
        // props.put("max.poll.records", 10000);

        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private Tuple2<byte[], String> myGetNamespaceKeyStateNameTuple(){
        return new Tuple2<byte[], String>(
            keyBuilder.buildCompositeKeyNamespace(
                state.getCurrentNamespace(), 
                state.getNamespaceSerializer()
            ),
            stateName
        );
    }

    private V getValue(K key){

        keyBuilder.setKeyAndKeyGroup(key, 0);

        try {
            Tuple2<byte[], String> namespaceKeyStateNameTuple = this.myGetNamespaceKeyStateNameTuple();

            byte[] valueBytes =
                    backend.namespaceKeyStatenameToValue.get(namespaceKeyStateNameTuple);
            if (valueBytes == null) {
                return (V) state.getDefaultValue();
            }

            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);

        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException(
                    "Error while retrieving data in getValue() in RedpandaConsumer.", e);
        }
    }

    private void makeUpdate(K key, V value){

        keyBuilder.setKeyAndKeyGroup(key, 0);
        // System.out.println("TEST 1");
        if (value == null) {
            // TODO: unimplemented
            // clear();
            return;
        }
        try {
            dataOutputView.clear();
            // System.out.println("TEST 2");
            state.valueSerializer.serialize((Long) value, dataOutputView);
            // System.out.println("TEST 3");
            byte[] serializedValue = dataOutputView.getCopyOfBuffer();
            // System.out.println("TEST 4");

            Tuple2<byte[], String> namespaceKeyStateNameTuple = this.myGetNamespaceKeyStateNameTuple();
            backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple, serializedValue);

            //            Fixed bug where we were using the wrong tuple to update the keys
            dataOutputView.clear();
            state.getNamespaceSerializer().serialize(state.getCurrentNamespace(), dataOutputView);
            byte[] currentNamespace = dataOutputView.getCopyOfBuffer();

            Tuple2<ByteBuffer, String> tupleForKeys =
                    new Tuple2(ByteBuffer.wrap(currentNamespace), stateName);
            HashSet<K> keyHash =
                    backend.namespaceAndStateNameToKeys.getOrDefault(
                            tupleForKeys, new HashSet<K>());
            keyHash.add(key);

            backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);

            backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this.state);
            backend.stateNamesToKeysAndNamespaces
                    .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
                    .add(namespaceKeyStateNameTuple.f0);

            // // persist to Redpanda
            // // TODO: need right topic
            // this.writeMessage(namespaceKeyStateNameTuple.f1, value);
        } catch (java.lang.Exception e) {
            System.out.println("ERROR");
            System.out.println(e);
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    private void processRecord(ConsumerRecord<String, String> record){
        // System.out.println();
        // System.out.printf("Processing Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
        //         record.partition(), record.offset());

        try{
            // get the current state for the word and add 1 to it
            // System.out.printf("Retrieved state value: %d\n", curr);
            // if(curr == null){
            //     curr = 0L;
            // }
            String word_key = record.key();
            Long value = Long.parseLong(record.value());
            
            // System.out.println("PROCESSING RECORD BEFORE MAKE UPDATE");
            // this.makeUpdate((K) word_key, (V) value);
            this.makeUpdate((K) Long.valueOf(word_key), (V) value);
            
            // System.out.println("PROCESSING RECORD AFTER MAKE UPDATE");
            // Latency testing: after this point, the new value is available in the user-code
            if(curr_records < num_records){
                long currentTime = System.currentTimeMillis();

                total_latency_from_produced += (currentTime - record.timestamp());
                assert(currentTime - record.timestamp() > 0);

                //I changed the keys to be words so I'll comment out this latency test. We'll need to change our benchmarks anyway
                // total_latency_from_source += (currentTime - record.key());
                // assert(currentTime - record.key() > 0);
                
                curr_records += 1;
            }

            // if((curr_records % 1000 == 0) && (curr_records < num_records)){
            //     System.out.println("===LATENCY TESTING RESULTS===");
            //     System.out.printf("Total Latency (from Producer): %f\n", 
            //         (float) total_latency_from_produced);
            //     // System.out.printf("Total Latency (from WordSource): %f\n",
            //     //     (float) total_latency_from_source);
            //     System.out.printf("Average Latency (from Producer): %f\n", 
            //         (float) total_latency_from_produced / curr_records);
            //     // System.out.printf("Average Latency (from WordSource): %f\n",
            //     //     (float) total_latency_from_source / curr_records);
            //     System.out.printf("Records processed: %d\n", curr_records);
            // }
            // System.out.printf("updated state for %s to %d from %d\n", word_key, state.value(), curr);
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
        // System.out.println("debug classloader in redpandaconsumer");
        // System.out.println(cl);
        // System.out.println(org.apache.kafka.common.utils.Utils.class.getClassLoader()); 

        try {
            cl.loadClass("org.apache.kafka.clients.NetworkClient$1");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler$Builder");
            cl.loadClass("org.apache.kafka.clients.FetchSessionHandler$FetchRequestData");

            cl.loadClass("org.apache.kafka.clients.consumer.OffsetAndMetadata");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerPartitionAssignor$GroupSubscription");

            cl.loadClass("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$OffsetCommitCompletion");
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

            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerRecord");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerRecords$ConcatenatedIterable");
            cl.loadClass("org.apache.kafka.clients.consumer.ConsumerRecords$ConcatenatedIterable$1");
            cl.loadClass("org.apache.kafka.clients.consumer.internals.ConsumerCoordinator$2");
            cl.loadClass("org.apache.kafka.clients.consumer.RetriableCommitFailedException");

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
    
    // NOTE: this does not reset the key to what is was before
    // not sure if it is possible to reset the key
    public void run() {

        keySerializer = (TypeSerializer<K>) state.keySerializer; //(TypeSerializer<K>) new StringSerializer();
        valueSerializer = (TypeSerializer<V>) state.valueSerializer; // (TypeSerializer<V>) new LongSerializer();

        while (true) {
            // System.out.println("[REDPANDACONSUMER] About to poll!");
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(100L);
            // System.out.println("[REDPANDACONSUMER] I am polling!");
            if (consumerRecords.count() != 0) {

                // System.out.println("Num consumer records " + consumerRecords.count());

                consumerRecords.forEach(record -> processRecord(record));
                consumer.commitAsync();
                break;
            }
            else {
                // System.out.println("No records.");
            }
        }
    }
}