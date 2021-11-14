/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

// Redpanda imports
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import java.util.Properties;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class RedpandaValueState<K, N, V> extends AbstractRedpandaState<K, N, V>
        implements InternalValueState<K, N, V> {

    private KafkaProducer<String, V> producer;
    private KafkaConsumer<String, String> consumer;
    private final static String TOPIC = "word_chat";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    // Our Redpanda thread
    public RedpandaConsumer thread;


    /**
     * Creates a new {@code RedpandaValueState}.
     *
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RedpandaValueState(
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            TypeSerializer<K> keySerializer,
            V defaultValue,
            RedpandaKeyedStateBackend<K> backend) {

        super(namespaceSerializer, valueSerializer, keySerializer, defaultValue, backend);

        // Create Redpanda producer
        this.producer = this.createProducer();
        // this.consumer = this.createConsumer();
        // this.writeMessage("word_chat", "word", (V) "1");
        // this.readRecords();

        // this.consumer.close();

        this.thread = new RedpandaConsumer<>(this.backend, this);
        this.thread.initialize();
        this.thread.setPriority(10);
        this.thread.start();

        // ClassLoader cl = Thread.currentThread().getContextClassLoader();
        // System.out.println("debug classloader in valuestate");
        // System.out.println(cl);
        // System.out.println(org.apache.kafka.common.utils.Utils.class.getClassLoader());
        // this.thread.setContextClassLoader(cl);
    }

    private KafkaProducer<String, V> createProducer() {
        // Properties gives properties to the KafkaProducer constructor, i.e. configuring the serialization
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "RedpandaProducer (ValueState)");

        //Used String Keys and commented out version with Long keys
        // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        //                                 LongSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());

        // TODO: temporary types
        return new KafkaProducer<String, V>(props);
    }

    private KafkaConsumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "RedpandaConsumer (ValueState)");
        // props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        //         LongDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
  
        // Create the consumer using props.
        final KafkaConsumer<String, String> consumer =
                                    new KafkaConsumer<>(props);
  
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }


    private boolean writeMessage(String TOPIC, String key, V value) {

        final ProducerRecord<String, V> record =
                        new ProducerRecord<String, V>(TOPIC, key, value);

        try {
            // System.out.println("ABOUT TO SEND RECORD");
            final RecordMetadata metadata = this.producer.send(record).get(); 
            // System.out.println(metadata);
            // System.out.println("SENT RECORD");
            // System.out.println();
        }
        catch(Exception e) {
            System.out.println("ERROR SENDING RECORD");
            System.out.println(e);
            return false;
        }
        finally {
            this.producer.flush();
            return true;
        }
    }

    private boolean readRecords() {

        final ConsumerRecords<String, String> consumerRecords =
                consumer.poll(10);
        System.out.println("in readRecords");
        System.out.println(consumerRecords.count());
        if (consumerRecords.count() != 0){
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                this.update_((V) record.value());

            });
    
            consumer.commitAsync();
        }
        return true;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    @Override
    public V value() {

        // System.out.println("current key (should be last key, unaffected by redpanda): " + this.backend.getCurrentKey());
        // this.thread.run();

        try {
            byte[] valueBytes =
                    backend.namespaceKeyStatenameToValue.get(getNamespaceKeyStateNameTuple());
            if (valueBytes == null) {
                return getDefaultValue();
            }
            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);
        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException(
                    "Error while retrieving data from Memory Mapped File.", e);
        }
    }

    @Override
    public void update(V value) {
        // if (value == null) {
        //     clear();
        //     return;
        // }
        try {
            // byte[] serializedValue = serializeValue(value, valueSerializer);
            // Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
            // backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple, serializedValue);

            // //            Fixed bug where we were using the wrong tuple to update the keys
            // byte[] currentNamespace = serializeCurrentNamespace();

            // Tuple2<ByteBuffer, String> tupleForKeys =
            //         new Tuple2(ByteBuffer.wrap(currentNamespace), getStateName());
            // HashSet<K> keyHash =
            //         backend.namespaceAndStateNameToKeys.getOrDefault(
            //                 tupleForKeys, new HashSet<K>());
            // keyHash.add(backend.getCurrentKey());

            // backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);

            // backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
            // backend.stateNamesToKeysAndNamespaces
            //         .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
            //         .add(namespaceKeyStateNameTuple.f0);

            // // persist to Redpanda
            // System.out.println("UPDATING VALUE (WRITING THROUGH TO REDPANDA)");
            // System.out.printf("key: %s, value: %s\n", String.valueOf(backend.getCurrentKey()), String.valueOf(value));

            // currently topic is hard-coded to word_chat since that is what the consumer is subscribed to
            // This could possibly changed to the state decriptor name for the value state idk
            // this.writeMessage(namespaceKeyStateNameTuple.f1, value);
            this.writeMessage("word_chat", String.valueOf(backend.getCurrentKey()), (V) String.valueOf(value));
        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    // update without the Redpanda reading
    public void update_(V value) {
        if (value == null) {
            clear();
            return;
        }
        try {
            byte[] serializedValue = serializeValue(value, valueSerializer);
            Tuple2<byte[], String> namespaceKeyStateNameTuple = getNamespaceKeyStateNameTuple();
            backend.namespaceKeyStatenameToValue.put(namespaceKeyStateNameTuple, serializedValue);

            //            Fixed bug where we were using the wrong tuple to update the keys
            byte[] currentNamespace = serializeCurrentNamespace();

            Tuple2<ByteBuffer, String> tupleForKeys =
                    new Tuple2(ByteBuffer.wrap(currentNamespace), getStateName());
            HashSet<K> keyHash =
                    backend.namespaceAndStateNameToKeys.getOrDefault(
                            tupleForKeys, new HashSet<K>());
            keyHash.add(backend.getCurrentKey());

            backend.namespaceAndStateNameToKeys.put(tupleForKeys, keyHash);

            backend.namespaceKeyStateNameToState.put(namespaceKeyStateNameTuple, this);
            backend.stateNamesToKeysAndNamespaces
                    .getOrDefault(namespaceKeyStateNameTuple.f1, new HashSet<byte[]>())
                    .add(namespaceKeyStateNameTuple.f0);

        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer)
            throws Exception {

        String stateName = backend.stateToStateName.get(this);
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyAndNamespace.f0, backend.getNumberOfKeyGroups());
        SerializedCompositeKeyBuilder<K> keyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);
        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
        byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);

        dataOutputView.clear();
        safeValueSerializer.serialize(getDefaultValue(), dataOutputView);
        byte[] defaultValue = dataOutputView.getCopyOfBuffer();

        byte[] value =
                backend.namespaceKeyStatenameToValue.getOrDefault(
                        new Tuple2<byte[], String>(key, stateName), defaultValue);

        return value;
    }

    @SuppressWarnings("unchecked")
    public static <K, N, NS, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            RegisteredKeyValueStateBackendMetaInfo<NS, SV> registerResult,
            TypeSerializer<K> keySerializer,
            RedpandaKeyedStateBackend<K> backend) {
        return (IS)
                new RedpandaValueState<>(
                        registerResult.getNamespaceSerializer(),
                        registerResult.getStateSerializer(),
                        keySerializer,
                        stateDesc.getDefaultValue(),
                        backend);
    }
}
