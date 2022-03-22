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

import java.util.Collections;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.*;
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
import org.apache.kafka.common.KafkaFuture;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

// Redpanda imports
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer; // rcord key serializer
import org.apache.kafka.common.serialization.StringSerializer; // record value serializer

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;

// ChronicleMap imports
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;


// Jiffy Client imports
import jiffy.JiffyClient;
import org.apache.flink.contrib.streaming.state.utils.InetAddressLocalHostUtil;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

/**
 * {@link ValueState} implementation that stores state in a Memory Mapped File.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RedpandaValueState<K, N, V> extends AbstractRedpandaState<K, N, V>
        implements InternalValueState<K, N, V> {

    private static Logger log = Logger.getLogger("mmf value state");
    ChronicleMap<K, V> kvStore;
    private static int numKeyedStatesBuilt = 0;
    boolean chronicleMapInitialized = false;

    KafkaProducer<K, V> producer;    
    KafkaConsumer<String, Long> offsetConsumer;
    public String key_class_name;
    public String value_class_name;
    //  if false, uses synchronous writes to Redpanda (lower latency and throughput)
    public boolean BATCH_WRITES = false;
    // if false, writes directly to chroniclemap and doesn't write to redpanda (for testing)
    public boolean USE_REDPANDA = true;


    public String TOPIC; // if not set, defaults to memory address of this object
    private final static String BOOTSTRAP_SERVERS = "localhost:9192";
    String hostAddress;
    AdminClient admin;

    // Our Redpanda thread
    public RedpandaConsumer<K, V, N> thread;

    // Jiffy integration
    JiffyClient client;
    public String directory_daemon_address;

    // Snapshotting
    public Long last_sent;
    public Long last_consumed_by_query_engine;
    Long num_sent = 0L;
    public Long checkpointing_interval = 10L; // time between checkpoints
    Long last_checkpoint = 0L;
    Collection<ConsumerGroupListing> groups;
    KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> offsets;


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
        System.out.println("Topic not configured, defaulting to: " + TOPIC);
    }

    public void setUpComponents() throws IOException {

        // Setup Jiffy connection
        try {
            System.out.println("Trying to connect to Jiffy at address: " + directory_daemon_address);
            this.client = new JiffyClient(directory_daemon_address, 9090, 9091);
        } catch (Exception e) {
            System.out.println("Failed to connect to Jiffy with client, are the Jiffy directory and storage daemons running?");
            System.out.println(e);
            System.exit(-1);
        }

        // Setup ChronicleMap
        this.kvStore = createChronicleMap();
        // Create Redpanda producer
        this.producer = this.createProducer();
        // Create Redpanda offset consumer
        this.offsetConsumer = (KafkaConsumer<String, Long>) this.createOffsetConsumer();
        // Create an AdminClient 
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            directory_daemon_address+":9192");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "ValueStateAdmin");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 300);
        this.admin = AdminClient.create(props);

        // Set up a consumer if we also want to read from Redpanda
        if(USE_REDPANDA){
        
            // Startup the Redpanda Consumer as an async thread
            // this.thread = new RedpandaConsumer<>(this.backend, this);
            // this.thread.setName("RedpandaConsumer-thread");
            // this.thread.initialize();
            // this.thread.setPriority(10);
            // this.thread.start();
        }
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
            cmapBuilder.averageKeySize(6); // average english word size
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
                + this.toString() + "/"
                + filePrefixes[i] 
                + "_"
                + Integer.toString(this.numKeyedStatesBuilt)
                + ".txt"
            );

            try {
                this.hostAddress = InetAddressLocalHostUtil.getLocalHostAsString();
                String hostAddress_ = InetAddress.getLocalHost().getHostAddress();
                System.out.println("Got the local host address as: " + hostAddress);
                System.out.println("[OLD] Got the local host address as: " + hostAddress_);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        

            try {
                client.createFile(filePath, "local://tmp", hostAddress);
            } catch (Exception e) {
                System.out.println(e);
            }

            files[i] = new File("/tmp" + filePath);
            files[i].getParentFile().mkdirs();
        }
        return files;
    }

    private Consumer<String, Long> createOffsetConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, directory_daemon_address+":9192");

        String tag = this.toString().substring(this.toString().lastIndexOf("@")+1);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "OffsetConsumer (RPValueState)-"+tag);


        // performance configs
        props.put("session.timeout.ms", 30000);
        props.put("max.poll.interval.ms", 43200000);
        props.put("request.timeout.ms", 43205000);

        // props.put("max.poll.records", 250000);
        // props.put("fetch.max.bytes", 52428800);
        // props.put("max.partition.fetch.bytes", 52428800);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                    LongDeserializer.class.getName());
        offsetConsumer = new KafkaConsumer<String, Long>(props);

        // Subscribe to the topic.
        offsetConsumer.subscribe(Collections.singletonList(TOPIC+"Offsets"));
        return offsetConsumer;
    }

    private KafkaProducer<K, V> createProducer() {

        // Configuring the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                            directory_daemon_address+":9192");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "RedpandaProducer (ValueState)");

        this.key_class_name = ((Class<K>) backend.getCurrentKey().getClass()).getName();
        this.value_class_name = ((Class<V>) valueSerializer.createInstance().getClass()).getName();

        System.out.println("Setting up producer: " + key_class_name + " " + value_class_name);
        // https://stackoverflow.com/questions/51521737/apache-kafka-linger-ms-and-batch-size-settings
        // https://stackoverflow.com/questions/66045267/kafka-setting-high-linger-ms-and-batch-size-not-helping
        // 1MB, 50ms linger gives good throughput
        if(BATCH_WRITES){
            System.out.println("Batching writes before sending them to Redpanda");
            props.put("batch.size", 1024*1024);//100*1024);//1024*1024);
            // props.put("buffer.size", 1024*1024);
            props.put("linger.ms", 10);
        }

        // for improving synchronous writing
        props.put("acks", "1"); // acknowledgement only from leader broker
        props.put("max.in.flight.requests.per.connection", "1"); // ordering guarantees

        // Handle dynamic types, though String may be enough for query engines (convert strings to json)
        if(key_class_name == "java.lang.String" && value_class_name == "java.lang.String"){
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class.getName());
            return (KafkaProducer<K, V>) new KafkaProducer<String, String>(props);
        }
        else if(key_class_name == "java.lang.String" && value_class_name == "java.lang.Long"){
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                        LongSerializer.class.getName());
            return (KafkaProducer<K, V>) new KafkaProducer<String, Long>(props);
        }
        else if(key_class_name == "java.lang.Long" && value_class_name == "java.lang.Long"){
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                        LongSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                        LongSerializer.class.getName());
            return (KafkaProducer<K, V>) new KafkaProducer<Long, Long>(props);
        }
        else{
            String error_message = String.format("Type combination %s and %s not supported yet.", key_class_name, value_class_name);
            throw new java.lang.UnsupportedOperationException(error_message);
        }
    }

    private boolean writeMessage(String TOPIC, K key, V value) {

        // Checkpoint every so often if async mode,
        // reducing producer throughput but increasing data freshness

        // BATCH_WRITES
        if(System.currentTimeMillis() - last_checkpoint > checkpointing_interval){
            try {
                
                Long start = System.currentTimeMillis();
                checkpoint();
                last_checkpoint = System.currentTimeMillis();
                System.out.println("[CHECKPOINT]: " + (last_checkpoint-start));
            } catch (Exception e) {
                //TODO: handle exception
            }
        }

        final ProducerRecord<K, V> record;

        this.num_sent += 1;

        // TODO - get this out of the hot path via Java's equivalent of templating
        if(key_class_name == "java.lang.String" && value_class_name == "java.lang.String"){
            record = (ProducerRecord<K, V>) new ProducerRecord<String, String>(
                TOPIC, 
                key.toString(), 
                value.toString()
            );
        }
        else if(key_class_name == "java.lang.String" && value_class_name == "java.lang.Long"){
            record = (ProducerRecord<K, V>) new ProducerRecord<String, Long>(
                TOPIC, 
                key.toString(), 
                (Long) value
            );
        }
        else if(key_class_name == "java.lang.Long" && value_class_name == "java.lang.Long"){
            record = (ProducerRecord<K, V>) new ProducerRecord<Long, Long>(
                TOPIC, 
                (Long) key, 
                (Long) value
            );
        }
        else{
            String error_message = String.format("Type combination %s and %s not supported yet.", key_class_name, value_class_name);
            throw new java.lang.UnsupportedOperationException(error_message);
        }

        record.headers().add("origin", this.hostAddress.getBytes(StandardCharsets.UTF_8));

        try {
            if(BATCH_WRITES == false){
                this.producer.send(record).get();
            }
            else{
                this.producer.send(record, new Callback(){
                    public void onCompletion(RecordMetadata recordMetadata, Exception e){
                        if(e == null){
                            RedpandaValueState.this.last_sent = recordMetadata.offset();
                        }
                        else{
                            System.out.println("ERROR in async send callback");
                        }
                    }
                });
            }
        }
        catch(Exception e) {
            System.out.println("ERROR SENDING RECORD");
            System.out.println(e);
            return false;
        }
        return true;
    }

    // public void async_checkpoint() throws InterruptedException, ExecutionException{
    //     // prefetch from kafka admin to save time
    //     if(groups == null)
    //         groups = this.admin.listConsumerGroups().all().get();
        
    //     for (ConsumerGroupListing consumerGroupListing : groups) {

    //         // dont send a request unless last one has been processed
    //         if(offsets == null){
    //             offsets = this.admin
    //                     .listConsumerGroupOffsets(consumerGroupListing.groupId())
    //                     .partitionsToOffsetAndMetadata();
    //         }
    //     }
    // }

    private void processRecord(ConsumerRecord<String, Long> record){
        last_consumed_by_query_engine = record.value();
    }

    public void checkpoint() throws InterruptedException, ExecutionException{

        boolean flag = true;
        
        while(flag){
            flag = false;

            try{
                final ConsumerRecords<String, Long> consumerRecords = offsetConsumer.poll(1L);
                if (consumerRecords.count() != 0) {
                    consumerRecords.forEach(record -> processRecord(record));
                    offsetConsumer.commitAsync();
                    System.out.println("Offset from consumer group: " + this.last_consumed_by_query_engine + "/" + this.last_sent);
                    if(this.last_consumed_by_query_engine < this.last_sent){
                        flag = true;
                    }
                }
            }
            catch (Exception e){}
        }
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
            setUpComponents();
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
    public void update(V value) throws IOException {

        if (!this.chronicleMapInitialized) {
            setUpComponents();
            this.chronicleMapInitialized = true;
        }

        // Always write to local ChronicleMap first
        try {
            this.kvStore.put(backend.getCurrentKey(), value);
        } 
        catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while adding data to Memory Mapped File", e);
        }

        // Write asynchronously to Redpanda
        try {
            this.writeMessage(TOPIC, backend.getCurrentKey(), value);
        } catch (java.lang.Exception e) {
            throw new FlinkRuntimeException("Error while writing data to Redpanda/Kafka", e);
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
