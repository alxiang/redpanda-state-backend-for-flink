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
import org.apache.flink.runtime.state.VoidNamespace;
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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;
import java.util.Map;

// Jiffy imports (TEMP: hacky way of accessing jiffy, i.e. putting the folder in directory)
import jiffy.storage.HashTableClient;
import jiffy.JiffyClient;
import jiffy.notification.HashTableListener;
import jiffy.notification.event.Notification;
import jiffy.util.ByteBufferUtils;

import org.apache.thrift.TException;

/**
 * {@link ValueState} implementation that stores state in a Memory Mapped File.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class RedpandaValueState<K, N, V> extends AbstractRedpandaState<K, N, V>
        implements InternalValueState<K, N, V> {

    private static Logger log = Logger.getLogger("mmf value state");
    private static int numKeyedStatesBuilt = 0;

    private JiffyClient client;
    public HashTableClient kvStore;
    private boolean jiffyInitialized = false;
    private String className = "RedpandaValueState";

    private KafkaProducer<String, String> producer;
    private final static String TOPIC = "word_chat";
    private final static String BOOTSTRAP_SERVERS = "localhost:9192";

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
            RedpandaKeyedStateBackend<K> backend) 
            throws IOException {

        super(namespaceSerializer, valueSerializer, keySerializer, defaultValue, backend);

        // Create Redpanda producer
        this.producer = this.createProducer();

        this.thread = new RedpandaConsumer<>(this.backend, this);
        this.thread.setName("RedpandaConsumer-thread");
        this.thread.initialize();
        this.thread.setPriority(10);
        // this.thread.start();
    }

    public void setUpJiffy() throws TException {
        this.kvStore = createJiffyHashTable();

        this.thread.start();
    }

    private HashTableClient createJiffyHashTable() throws TException {
        client = new JiffyClient("127.0.0.1", 9090, 9091);

        String filename = "/" + this.className + "/" + "namespaceKeyStateNameToValue" +".txt";
        client.createHashTable(filename, "local://tmp", 1, 1);

        HashTableClient kv = client.openHashTable(filename);
        return kv;
    }

    private KafkaProducer<String, String> createProducer() {
        // Properties gives properties to the KafkaProducer constructor, i.e. configuring the serialization
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "RedpandaProducer (ValueState)");

        // Using String keys always
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    StringSerializer.class.getName());

        // https://stackoverflow.com/questions/51521737/apache-kafka-linger-ms-and-batch-size-settings
        // https://stackoverflow.com/questions/66045267/kafka-setting-high-linger-ms-and-batch-size-not-helping
        // 1MB, 50ms linger gives good throughput
        // compression didn't help
        // props.put("batch.size", 1024*1024);
        // props.put("buffer.size", 1024*1024);
        // props.put("linger.ms", 50);

        // for improving synchronous writing
        props.put("acks", "1"); // acknowledgement only from leader broker
        props.put("max.in.flight.requests.per.connection", "1"); // ordering guarantees

        // always send string records
        return new KafkaProducer<String, String>(props);
    }

    private boolean writeMessage(String TOPIC, String key, String value) {

        final ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(TOPIC, key, value);

        try {
            this.producer.send(record).get();
        }
        catch(Exception e) {
            System.out.println("ERROR SENDING RECORD");
            System.out.println(e);
            return false;
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
    public Set<K> getKeys(N n) {
        return null;
        // return kvStore.keySet();
    }

    @Override
    public V value() throws IOException {

        if (!this.jiffyInitialized) {
            try {
                setUpJiffy();
            } catch (Exception e) {
                //TODO: handle exception
                System.out.println("Failed to set up jiffy");
                System.exit(-1);
            }
            this.jiffyInitialized = true;
        }

        K backendKey = backend.getCurrentKey();
        ByteBuffer key = ByteBufferUtils.fromString("test_key"); //ByteBufferUtils.fromLong((long) backendKey);

        try {
            System.out.println("Checking if key exists");
            if (!kvStore.exists(key)) {
                System.out.println("Key doesn't exist");
                return defaultValue;
            }
    
            System.out.println("Trying to retrieve value");
            ByteBuffer value = this.kvStore.get(key);

            System.out.printf("retrieved %b value from jiffy\n", value);
    
            return (V) (Long) ByteBufferUtils.toLong(value);
        } catch (Exception e) {
            //TODO: handle exception
            System.out.println("failed to retrieve from jiffy!");
            System.out.println(e);
            System.exit(-1);
        }

        return null;
    }

    Tuple2<K, N> getBackendKey() {
        return new Tuple2<K, N>(backend.getCurrentKey(), getCurrentNamespace());
    }

    @Override
    public void update(V value) {

        try {
            this.writeMessage(TOPIC, backend.getCurrentKey().toString(), value.toString());
        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }
    }

    // TEMP: this doesn't really work, but we don't need it?
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
        // if (kvStore.containsKey(keyAndNamespace.f0)) {
        //     V value = kvStore.get(keyAndNamespace.f0);
        //     dataOutputView.clear();
        //     safeValueSerializer.serialize(value, dataOutputView);
        //     return dataOutputView.getCopyOfBuffer();
        // }

        dataOutputView.clear();
        safeValueSerializer.serialize(getDefaultValue(), dataOutputView);
        byte[] defaultValue = dataOutputView.getCopyOfBuffer();

        return defaultValue;

        // byte[] value =
        //         backend.namespaceKeyStatenameToValue.getOrDefault(
        //                 new Tuple2<byte[], String>(key, stateName), defaultValue);

        // return value;
    }

    @SuppressWarnings("unchecked")
    public static <K, N, NS, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            RegisteredKeyValueStateBackendMetaInfo<NS, SV> registerResult,
            TypeSerializer<K> keySerializer,
            RedpandaKeyedStateBackend<K> backend) 
            throws IOException {
        return (IS)
                new RedpandaValueState<>(
                        registerResult.getNamespaceSerializer(),
                        registerResult.getStateSerializer(),
                        keySerializer,
                        stateDesc.getDefaultValue(),
                        backend);
    }
}
