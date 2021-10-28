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

    /** Serializer for the namespace. */
    final TypeSerializer<N> namespaceSerializer;
    /** Serializer for the state values. */
    final TypeSerializer<V> valueSerializer;

    final TypeSerializer<K> keySerializer;

    private final SerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

    // configured!
    final String stateName;
    final N currentNamespace;

    public RedpandaConsumer(
        RedpandaKeyedStateBackend<K> keyedBackend
    ) {
        backend = keyedBackend;
        this.consumer = createConsumer();

        // setup the data views
        this.dataOutputView = new DataOutputSerializer(128);
        this.dataInputView = new DataInputDeserializer();

        sharedKeyNamespaceSerializer = backend.getSharedKeyBuilder();

        // For PrintingJob, this can be found in WordCountMap.open() and is 'Word counter'
        stateName = "Word counter"; 
        // For PrintingJob, the namespace is VoidNamespace.
        // We can tell this by printing the namespace in ValueState.value()
        // ex: https://www.programcreek.com/java-api-examples/?api=org.apache.flink.runtime.state.VoidNamespace
        currentNamespace = (N) VoidNamespace.INSTANCE;
        namespaceSerializer = (TypeSerializer<N>) VoidNamespaceSerializer.INSTANCE;
               
        // These should be user configured
        keySerializer = (TypeSerializer<K>) new StringSerializer();
        valueSerializer = (TypeSerializer<V>) new LongSerializer();
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

    public byte[] serializeValue(V value, TypeSerializer<V> safeValueSerializer) throws Exception {
        dataOutputView.clear();
        safeValueSerializer.serialize(value, dataOutputView);
        return dataOutputView.getCopyOfBuffer();
    }

    public byte[] serializeNamespace(N namespace, TypeSerializer<N> safeValueSerializer)
            throws Exception {
        dataOutputView.clear();
        safeValueSerializer.serialize(namespace, dataOutputView);
        return dataOutputView.getCopyOfBuffer();
    }

    private void processRecord(ConsumerRecord<Long, String> record){
        System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
                record.partition(), record.offset());

        try{
            // 0.1. Instantiate namespaceKeyStateNameTuple
            // - assumes we know the currentNamespace and the stateName
            Tuple2<byte[], String> namespaceKeyStateNameTuple = new Tuple2<byte[], String>(
                sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(currentNamespace, namespaceSerializer), stateName
            );
            // 0.2. Instantiate tupleForKeys
            byte[] serializedCurrentNamespace = serializeNamespace(currentNamespace, namespaceSerializer);
            Tuple2<ByteBuffer, String> tupleForKeys = 
                new Tuple2(ByteBuffer.wrap(serializedCurrentNamespace), stateName);
                
            // 1. Get the serialized value and put it into namespaceKeyStatenameToValue
            // - assumes we know type of valueSerializer
            byte[] serializedValue = serializeValue((V) record.value(), valueSerializer); 
            backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple, serializedValue);

            // 2. Try to get a ValueState corresponding to the key in record,
            //    and instantiate a new ValueState if one doesn't exist for the corresponding key
            org.apache.flink.api.common.state.State ValueState = 
                backend.namespaceKeyStateNameToState.get(namespaceKeyStateNameTuple); // key line
            if(ValueState == null){
                // need to create state here
                // ValueState = new RedpandaValueState<>(
                //         namespaceSerializer,
                //         null,//StateSerializer,
                //         keySerializer,
                //         0L,
                //         backend);
                backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, ValueState);
            }

            // 3. Add the record's value to the keyHash and put the keyHash in the backend
            HashSet<K> keyHash =
                    backend.namespaceAndStateNameToKeys.getOrDefault(
                            tupleForKeys, new HashSet<K>());
            keyHash.add((K) record.value());//backend.getCurrentKey());
            backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);

            backend.stateNamesToKeysAndNamespaces
                    .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
                    .add(namespaceKeyStateNameTuple.f0);
        }
        catch (Exception exception){
            System.out.println("Exception in processRecord(): " + exception);
        }

        
        
    }
    
    public void run() {
        
        Integer i = 0;
        while (i < 10) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000L);

            System.out.println("Polling in RedpandaConsumer...");

            if (consumerRecords.count() != 0) {

                consumerRecords.forEach(record -> processRecord(record));
                consumer.commitAsync();
            }
            else {
                System.out.println("No records.");
            }

            i += 1;
        }
    }
}