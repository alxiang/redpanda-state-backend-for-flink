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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;
import java.util.Map;

import jiffy.JiffyClient;
import jiffy.storage.FileWriter;
import jiffy.storage.FileReader;
import jiffy.storage.HashTableClient;
import jiffy.notification.HashTableListener;
import jiffy.directory.directory_service.Client;
import jiffy.notification.event.Notification;
import jiffy.util.ByteBufferUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
    ChronicleMap<K, V> kvStore;
    private static int numKeyedStatesBuilt = 0;
    private boolean chronicleMapInitialized = false;
    private String className = "RedpandaValueState";

    private KafkaProducer<String, String> producer;
    // TODO(ALEC): see if "this.toString()" is sufficient ofr unique naming
    String TOPIC;
    private final static String BOOTSTRAP_SERVERS = "localhost:9192";

    // Our Redpanda thread
    public RedpandaConsumer thread;

    // Jiffy integration
    JiffyClient client;

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

        // programmatic definition of topic for Redpanda integration
        // using memory address of the current class to avoid collisions
        TOPIC = this.toString().substring(this.toString().lastIndexOf("@")+1);

        // Create Redpanda producer
        this.producer = this.createProducer();

        this.thread = new RedpandaConsumer<>(this.backend, this);
        this.thread.setName("RedpandaConsumer-thread");
        this.thread.initialize();
        this.thread.setPriority(10);
        this.thread.start();

        try {
            this.client = new JiffyClient("127.0.0.1", 9090, 9091);
        } catch (Exception e) {
            System.out.println("Failed to connect to Jiffy with client, are the Jiffy directory and storage daemons running?");
            System.out.println(e);
            System.exit(-1);
        }
    }

    public void setUpChronicleMap() throws IOException {
        this.kvStore = createChronicleMap();
    }

    private ChronicleMap<K, V> createChronicleMap() throws IOException {
        String[] filePrefixes = {
            "namespaceKeyStateNameToValue",
        };
        File[] files = createPersistedFiles(filePrefixes);

        numKeyedStatesBuilt += 1;
        ChronicleMapBuilder<K, V> cmapBuilder =
                ChronicleMapBuilder.of(
                                (Class<K>) backend.getCurrentKey().getClass(),
                                (Class<V>) valueSerializer.createInstance().getClass())
                        .name("key-and-namespace-to-values")
                        .entries(1_000_000);
        if (backend.getCurrentKey() instanceof Integer || backend.getCurrentKey() instanceof Long) {
            log.info("Key is an Int or Long");
        } else {
            cmapBuilder.averageKeySize(64);
        }

        if (valueSerializer.createInstance() instanceof Integer
                || valueSerializer.createInstance() instanceof Long) {
            log.info("Value is an Int or Long");
        } else {
            cmapBuilder.averageValue(valueSerializer.createInstance());
        }
        return cmapBuilder.createPersistedTo(files[0]);
    }

    private File[] createPersistedFiles(String[] filePrefixes) throws IOException {
        File[] files = new File[filePrefixes.length];
        for (int i = 0; i < filePrefixes.length; i++) {

            String filePath = (
                "/BackendChronicleMaps/"
                + this.toString() + "/"  // TODO(ALEC): see if "this.toString()" is sufficient ofr unique naming
                + filePrefixes[i] 
                + "_"
                + Integer.toString(this.numKeyedStatesBuilt)
                + ".txt"
            );



            String hostAddress = "";
            try {
                hostAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        

            try {
                // TODO(ALEC): programmatically use the host_name for the local node here
                FileWriter writer = client.createFile(filePath, "local://tmp", hostAddress);
            } catch (Exception e) {
                System.out.println(e);
            }

            files[i] = new File("/tmp" + filePath);
            files[i].getParentFile().mkdirs();
        }
        return files;
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
        return kvStore.keySet();
    }

    @Override
    public V value() throws IOException {

        if (!this.chronicleMapInitialized) {
            setUpChronicleMap();
            this.chronicleMapInitialized = true;
        }

        K backendKey = backend.getCurrentKey();
        if (!kvStore.containsKey(backendKey)) {
            return defaultValue;
        }

        return this.kvStore.get(backendKey);
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

        // optimized version uses
        //  kvStore.remove(backend.getCurrentKey());
        // this.kvStore.put(backend.getCurrentKey(), value);
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
        if (kvStore.containsKey(keyAndNamespace.f0)) {
            V value = kvStore.get(keyAndNamespace.f0);
            dataOutputView.clear();
            safeValueSerializer.serialize(value, dataOutputView);
            return dataOutputView.getCopyOfBuffer();
        }

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
