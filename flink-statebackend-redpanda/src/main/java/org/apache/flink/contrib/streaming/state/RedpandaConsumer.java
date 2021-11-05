package org.apache.flink.contrib.streaming.state;

import java.util.Collections;
import java.util.Properties;

import com.typesafe.config.ConfigException.Null;

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
    private Consumer<Long, String> consumer;

    private final static String TOPIC = "word_chat";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    protected final DataOutputSerializer dataOutputView;
    protected final DataInputDeserializer dataInputView;   

    // /** Serializer for the namespace. */
    // final TypeSerializer<N> namespaceSerializer;
    // /** Serializer for the state values. */
    final TypeSerializer<V> valueSerializer;

    final TypeSerializer<K> keySerializer;

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
        RedpandaKeyedStateBackend<K> keyedBackend
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

        // For PrintingJob, the namespace is VoidNamespace.
        // We can tell this by printing the namespace in ValueState.value()
        // ex: https://www.programcreek.com/java-api-examples/?api=org.apache.flink.runtime.state.VoidNamespace
        // currentNamespace = (N) VoidNamespace.INSTANCE;
        // namespaceSerializer = (TypeSerializer<N>) VoidNamespaceSerializer.INSTANCE;
               
        // These should be user configured
        keySerializer = (TypeSerializer<K>) state.keySerializer; //(TypeSerializer<K>) new StringSerializer();
        valueSerializer = (TypeSerializer<V>) state.valueSerializer; // (TypeSerializer<V>) new LongSerializer();
    }

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "RedpandaPollingConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);

        // performance configs
        props.put("fetch.min.bytes", 100000000);
        props.put("max.poll.records", 10000);

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private V getValue(K key){

        keyBuilder.setKeyAndKeyGroup(key, 0);

        try {
            Tuple2<byte[], String> namespaceKeyStateNameTuple = new Tuple2<byte[], String>(
                keyBuilder.buildCompositeKeyNamespace(
                    state.getCurrentNamespace(), 
                    state.getNamespaceSerializer()
                ),
                stateName
            );

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

    private void processRecord(ConsumerRecord<Long, String> record){
        // System.out.println();
        // System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
        //         record.partition(), record.offset());

        try{
            String word_key = record.value();
            // get the current state for the word and add 1 to it
            Long curr = (Long) this.getValue((K) word_key);
            // System.out.printf("Retrieved state value: %d\n", curr);
            if(curr == null){
                curr = 0L;
            }

            // Using update_ instead of update so that this.run() is not called recursively
            state.update_(curr + 1);

            // Latency testing: after this point, the new value is available in the user-code
            if(curr_records < num_records){
                long currentTime = System.currentTimeMillis();

                total_latency_from_produced += (currentTime - record.timestamp());
                assert(currentTime - record.timestamp() > 0);

                total_latency_from_source += (currentTime - record.key());
                assert(currentTime - record.key() > 0);
                
                curr_records += 1;
            }

            if((curr_records % 1000 == 0) && (curr_records < num_records)){
                System.out.println("===LATENCY TESTING RESULTS===");
                System.out.printf("Total Latency (from Producer): %f\n", 
                    (float) total_latency_from_produced);
                System.out.printf("Total Latency (from WordSource): %f\n",
                    (float) total_latency_from_source);
                System.out.printf("Average Latency (from Producer): %f\n", 
                    (float) total_latency_from_produced / curr_records);
                System.out.printf("Average Latency (from WordSource): %f\n",
                    (float) total_latency_from_source / curr_records);
                System.out.printf("Records processed: %d\n", curr_records);
            }
            // System.out.printf("updated state for %s to %d from %d\n", word_key, state.value(), curr);
        }
        catch (Exception exception){
            System.out.println("Exception in processRecord(): " + exception);
        }
    }
    
    // NOTE: this does not reset the key to what is was before
    // not sure if it is possible to reset the key
    public void run() {

        if(this.consumer == null){
            this.consumer = createConsumer();
        }

        System.out.println("retrieving state from statename");
        state = (RedpandaValueState<K, N, Long>) backend.stateNameToState.get(stateName);
        System.out.println("retrieved: " + state);

        System.out.println("retrieving current key from state");
        K key = backend.getCurrentKey();
        System.out.println("retrieved: " + key);

        System.out.println("retrieving current key group from state");
        int keygroup = backend.getCurrentKeyGroupIndex();
        System.out.println("retrieved: " + keygroup);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        System.out.println("debug classloader");
        System.out.println(cl);
        System.out.println(org.apache.kafka.common.utils.Utils.class.getClassLoader()); 
        Integer i = 0;
        while (i < 1) {
            // System.out.println("Polling in RedpandaConsumer...");
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(0L);

            if (consumerRecords.count() != 0) {

                System.out.println("Num consumer records " + consumerRecords.count());

                consumerRecords.forEach(record -> processRecord(record));
                consumer.commitAsync();
                break;
            }
            else {
                // System.out.println("No records.");
            }

            i += 1;
        }

        // TODO: If we had the previous key, we could reset it like this
        // state.getSharedKeyNamespaceSerializer().setKeyAndKeyGroup((K) prev_key, 0);
    }
}