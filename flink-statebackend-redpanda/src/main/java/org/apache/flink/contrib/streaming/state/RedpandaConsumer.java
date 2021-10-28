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

    // protected final DataOutputSerializer dataOutputView;
    // protected final DataInputDeserializer dataInputView;   

    // /** Serializer for the namespace. */
    // final TypeSerializer<N> namespaceSerializer;
    // /** Serializer for the state values. */
    // final TypeSerializer<V> valueSerializer;

    // final TypeSerializer<K> keySerializer;

    private final SerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

    // configured!
    final String stateName;
    // final N currentNamespace;
    RedpandaValueState<K, N, Long> state;

    // For latency testing:
    // keep track of total latency over 1,000,000 records 
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
        this.consumer = createConsumer();

        // setup the data views
        // this.dataOutputView = new DataOutputSerializer(128);
        // this.dataInputView = new DataInputDeserializer();

        sharedKeyNamespaceSerializer = backend.getSharedKeyBuilder();

        // For PrintingJob, this can be found in WordCountMap.open() and is 'Word counter'
        stateName = "Word counter"; 

        // For PrintingJob, the namespace is VoidNamespace.
        // We can tell this by printing the namespace in ValueState.value()
        // ex: https://www.programcreek.com/java-api-examples/?api=org.apache.flink.runtime.state.VoidNamespace
        // currentNamespace = (N) VoidNamespace.INSTANCE;
        // namespaceSerializer = (TypeSerializer<N>) VoidNamespaceSerializer.INSTANCE;
               
        // These should be user configured
        // keySerializer = state.keySerializer; //(TypeSerializer<K>) new StringSerializer();
        // valueSerializer = state.valueSerializer; // (TypeSerializer<V>) new LongSerializer();
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

    private void processRecord(ConsumerRecord<Long, String> record){
        // System.out.println();
        // System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
        //         record.partition(), record.offset());

        try{
            String word_key = record.value();
            state.getSharedKeyNamespaceSerializer().setKeyAndKeyGroup((K) word_key, 0);

            // get the current state for the word and add 1 to it
            Long curr = state.value();
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
            if(curr_records == num_records && latency_printed == false){
                System.out.println("===LATENCY TESTING RESULTS===");
                System.out.printf("Number of samples: %d\n", num_records);
                System.out.printf("Average Latency (from Producer): %f\n", 
                    (float) total_latency_from_produced / num_records);
                System.out.printf("Average Latency (from WordSource): %f\n",
                    (float) total_latency_from_source / num_records);

                latency_printed = true;
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

        // System.out.println("retrieving state from statename");
        state = (RedpandaValueState<K, N, Long>) backend.stateNameToState.get(stateName);
        // System.out.println("retrieved: " + state);
        
        Integer i = 0;
        while (i < 1) {
            // System.out.println("Polling in RedpandaConsumer...");
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(100L);

            if (consumerRecords.count() != 0) {

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