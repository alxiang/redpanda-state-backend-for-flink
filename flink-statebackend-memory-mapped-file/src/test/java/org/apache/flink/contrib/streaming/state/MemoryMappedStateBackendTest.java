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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import junit.framework.TestCase;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/** Tests for the partitioned state part of {@link MemoryMappedStateBackend}. */
@RunWith(Parameterized.class)
public class MemoryMappedStateBackendTest extends TestLogger {

    private OneShotLatch blocker;
    private OneShotLatch waiter;
    private BlockerCheckpointStreamFactory testStreamFactory;
    private MemoryMappedKeyedStateBackend<Integer> keyedStateBackend;
    //    private List<RocksObject> allCreatedCloseables;
    private ValueState<Integer> testState1;
    private ValueState<String> testState2;

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Parameterized.Parameters
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {
                        true,
                        (SupplierWithException<CheckpointStorage, IOException>)
                                JobManagerCheckpointStorage::new
                    },
                    {
                        false,
                        (SupplierWithException<CheckpointStorage, IOException>)
                                () -> {
                                    String checkpointPath =
                                            TEMP_FOLDER.newFolder().toURI().toString();
                                    return new FileSystemCheckpointStorage(checkpointPath);
                                }
                    }
                });
    }

    @Parameterized.Parameter(value = 0)
    public boolean enableIncrementalCheckpointing;

    @Parameterized.Parameter(value = 1)
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    // Store it because we need it for the cleanup test.
    private String dbPath;
    private MockEnvironment env;

    private MockEnvironment buildMockEnv() {
        return MockEnvironment.builder().build();
    }

    @Before
    public void before() {
        env = buildMockEnv();
    }

    @After
    public void after() {
        IOUtils.closeQuietly(env);
    }

    /** Returns the value by getting the serialized value and deserializing it if it is not null. */
    protected static <V, K, N> V getSerializedValue(
            InternalKvState<K, N, V> kvState,
            K key,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer)
            throws Exception {

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key, keySerializer, namespace, namespaceSerializer);

        byte[] serializedValue =
                kvState.getSerializedValue(
                        serializedKeyAndNamespace,
                        kvState.getKeySerializer(),
                        kvState.getNamespaceSerializer(),
                        kvState.getValueSerializer());

        if (serializedValue == null) {
            return null;
        } else {
            return KvStateSerializer.deserializeValue(serializedValue, valueSerializer);
        }
    }

    protected MemoryMappedStateBackend getStateBackend() throws IOException {
        dbPath = TEMP_FOLDER.newFolder().getAbsolutePath();
        MemoryMappedStateBackend backend = new MemoryMappedStateBackend();
        Configuration configuration = new Configuration();
        backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
        return backend;
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer) throws Exception {
        return createKeyedBackend(keySerializer, env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer, Environment env) throws Exception {
        return createKeyedBackend(keySerializer, 10, new KeyGroupRange(0, 9), env);
    }

    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        CheckpointableKeyedStateBackend<K> backend =
                getStateBackend()
                        .createKeyedStateBackend(
                                env,
                                new JobID(),
                                "test_op",
                                keySerializer,
                                numberOfKeyGroups,
                                keyGroupRange,
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());

        return backend;
    }

    //    @Test
    @SuppressWarnings("unchecked")
    public void testValueState2() throws Exception {
        //        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

        ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

        TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            ValueState<String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<Integer, VoidNamespace, String> kvState =
                    (InternalKvState<Integer, VoidNamespace, String>) state;

            // this is only available after the backend initialized the serializer
            TypeSerializer<String> valueSerializer = kvId.getSerializer();

            // some modifications to the state
            backend.setCurrentKey(1);
            assertNull(state.value());
            assertNull(
                    getSerializedValue(
                            kvState,
                            1,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            state.update("1");
            backend.setCurrentKey(2);
            assertNull(state.value());
            assertNull(
                    getSerializedValue(
                            kvState,
                            2,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            state.update("2");
            backend.setCurrentKey(1);
            assertEquals("1", state.value());
            assertEquals(
                    "1",
                    getSerializedValue(
                            kvState,
                            1,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));

            // make some more modifications
            backend.setCurrentKey(1);
            state.update("u1");
            backend.setCurrentKey(2);
            state.update("u2");
            backend.setCurrentKey(3);
            state.update("u3");

            // validate the original state
            backend.setCurrentKey(1);
            assertEquals("u1", state.value());
            assertEquals(
                    "u1",
                    getSerializedValue(
                            kvState,
                            1,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            backend.setCurrentKey(2);
            assertEquals("u2", state.value());
            assertEquals(
                    "u2",
                    getSerializedValue(
                            kvState,
                            2,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            backend.setCurrentKey(3);
            assertEquals("u3", state.value());
            assertEquals(
                    "u3",
                    getSerializedValue(
                            kvState,
                            3,
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            valueSerializer));
            // Added for testing volume
            for (int i = 0; i < 100; i++) {
                backend.setCurrentKey(i);
                state.update(Integer.toString(i));
            }

            for (int i = 70; i > 0; i--) {
                backend.setCurrentKey(i);
                assertEquals(Integer.toString(i), state.value());
            }

            backend.dispose();

        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    //    @Test
    public void testGetKeys() throws Exception {
        final int namespace1ElementsNum = 500000;
        final int namespace2ElementsNum = 1000;
        String fieldName = "get-keys-test";
        CheckpointableKeyedStateBackend<Integer> backend =
                createKeyedBackend(IntSerializer.INSTANCE);
        try {
            final String ns1 = "ns1";
            ValueState<Integer> keyedState1 =
                    backend.getPartitionedState(
                            ns1,
                            StringSerializer.INSTANCE,
                            new ValueStateDescriptor<>(fieldName, IntSerializer.INSTANCE));

            for (int key = 0; key < namespace1ElementsNum; key++) {
                backend.setCurrentKey(key);
                keyedState1.update(key * 2);
            }

            final String ns2 = "ns2";
            ValueState<Integer> keyedState2 =
                    backend.getPartitionedState(
                            ns2,
                            StringSerializer.INSTANCE,
                            new ValueStateDescriptor<>(fieldName, IntSerializer.INSTANCE));

            for (int key = namespace1ElementsNum;
                    key < namespace1ElementsNum + namespace2ElementsNum;
                    key++) {
                backend.setCurrentKey(key);
                keyedState2.update(key * 2);
            }

            // valid for namespace1
            //            System.out.println(backend.getKeys(fieldName, ns1));
            try (Stream<Integer> keysStream = backend.getKeys(fieldName, ns1).sorted()) {
                PrimitiveIterator.OfInt actualIterator =
                        keysStream.mapToInt(value -> value.intValue()).iterator();

                for (int expectedKey = 0; expectedKey < namespace1ElementsNum; expectedKey++) {
                    assertTrue(actualIterator.hasNext());
                    assertEquals(expectedKey, actualIterator.nextInt());
                }

                assertFalse(actualIterator.hasNext());
            }

            // valid for namespace2
            try (Stream<Integer> keysStream = backend.getKeys(fieldName, ns2).sorted()) {
                PrimitiveIterator.OfInt actualIterator =
                        keysStream.mapToInt(value -> value.intValue()).iterator();

                for (int expectedKey = namespace1ElementsNum;
                        expectedKey < namespace1ElementsNum + namespace2ElementsNum;
                        expectedKey++) {
                    assertTrue(actualIterator.hasNext());
                    assertEquals(expectedKey, actualIterator.nextInt());
                }

                assertFalse(actualIterator.hasNext());
            }
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }

    @Test
    @SuppressWarnings("unchecked,rawtypes")
    public void testMapState() throws Exception {
        //        CheckpointStreamFactory streamFactory = createStreamFactory();
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

        MapStateDescriptor<Integer, String> kvId =
                new MapStateDescriptor<>("id", Integer.class, String.class);

        TypeSerializer<String> keySerializer = StringSerializer.INSTANCE;
        TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

        CheckpointableKeyedStateBackend<String> backend =
                createKeyedBackend(StringSerializer.INSTANCE);
        try {
            MapState<Integer, String> state =
                    backend.getPartitionedState(
                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
            @SuppressWarnings("unchecked")
            InternalKvState<String, VoidNamespace, Map<Integer, String>> kvState =
                    (InternalKvState<String, VoidNamespace, Map<Integer, String>>) state;

            // these are only available after the backend initialized the serializer
            TypeSerializer<Integer> userKeySerializer = kvId.getKeySerializer();
            TypeSerializer<String> userValueSerializer = kvId.getValueSerializer();

            // some modifications to the state
            backend.setCurrentKey("1");
            TestCase.assertNull(state.get(1));
            TestCase.assertNull(
                    getSerializedMap(
                            kvState,
                            "1",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));
            state.put(1, "1");
            backend.setCurrentKey("2");
            TestCase.assertNull(state.get(2));
            TestCase.assertNull(
                    getSerializedMap(
                            kvState,
                            "2",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));
            state.put(2, "2");
            // put entry with different userKeyOffset
            backend.setCurrentKey("11");
            state.put(11, "11");

            backend.setCurrentKey("1");
            assertTrue(state.contains(1));
            assertEquals("1", state.get(1));
            assertEquals(
                    new HashMap<Integer, String>() {
                        {
                            put(1, "1");
                        }
                    },
                    getSerializedMap(
                            kvState,
                            "1",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));
            assertEquals(
                    new HashMap<Integer, String>() {
                        {
                            put(11, "11");
                        }
                    },
                    getSerializedMap(
                            kvState,
                            "11",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));
            // make some more modifications
            backend.setCurrentKey("1");
            state.put(1, "101");
            backend.setCurrentKey("2");
            state.put(102, "102");
            backend.setCurrentKey("3");
            state.put(103, "103");
            state.putAll(
                    new HashMap<Integer, String>() {
                        {
                            put(1031, "1031");
                            put(1032, "1032");
                        }
                    });
            // validate the original state
            backend.setCurrentKey("1");
            assertEquals("101", state.get(1));
            assertEquals(
                    new HashMap<Integer, String>() {
                        {
                            put(1, "101");
                        }
                    },
                    getSerializedMap(
                            kvState,
                            "1",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));
            backend.setCurrentKey("2");
            assertEquals("102", state.get(102));
            assertEquals(
                    new HashMap<Integer, String>() {
                        {
                            put(2, "2");
                            put(102, "102");
                        }
                    },
                    getSerializedMap(
                            kvState,
                            "2",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));
            backend.setCurrentKey("3");
            assertTrue(state.contains(103));
            assertEquals("103", state.get(103));
            assertEquals(
                    new HashMap<Integer, String>() {
                        {
                            put(103, "103");
                            put(1031, "1031");
                            put(1032, "1032");
                        }
                    },
                    getSerializedMap(
                            kvState,
                            "3",
                            keySerializer,
                            VoidNamespace.INSTANCE,
                            namespaceSerializer,
                            userKeySerializer,
                            userValueSerializer));

            List<Integer> keys = new ArrayList<>();
            for (Integer key : state.keys()) {
                keys.add(key);
            }
            List<Integer> expectedKeys = Arrays.asList(103, 1031, 1032);
            assertEquals(keys.size(), expectedKeys.size());
            keys.removeAll(expectedKeys);

            List<String> values = new ArrayList<>();
            for (String value : state.values()) {
                values.add(value);
            }
            List<String> expectedValues = Arrays.asList("103", "1031", "1032");
            assertEquals(values.size(), expectedValues.size());
            values.removeAll(expectedValues);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }
    }
    /** Returns the value by getting the serialized value and deserializing it if it is not null. */
    private static <UK, UV, K, N> Map<UK, UV> getSerializedMap(
            InternalKvState<K, N, Map<UK, UV>> kvState,
            K key,
            TypeSerializer<K> keySerializer,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer)
            throws Exception {

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key, keySerializer, namespace, namespaceSerializer);

        byte[] serializedValue =
                kvState.getSerializedValue(
                        serializedKeyAndNamespace,
                        kvState.getKeySerializer(),
                        kvState.getNamespaceSerializer(),
                        kvState.getValueSerializer());

        if (serializedValue == null) {
            return null;
        } else {
            return KvStateSerializer.deserializeMap(
                    serializedValue, userKeySerializer, userValueSerializer);
        }
    }

    public static <K> MemoryMappedKeyedStateBackendBuilder<K> builderForTestDB(
            File instanceBasePath, TypeSerializer<K> keySerializer) {
        return new MemoryMappedKeyedStateBackendBuilder<>(
                "no-op",
                ClassLoader.getSystemClassLoader(),
                new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                keySerializer,
                2,
                new KeyGroupRange(0, 1),
                new ExecutionConfig(),
                TestLocalRecoveryConfig.disabled(),
                TtlTimeProvider.DEFAULT,
                LatencyTrackingStateConfig.disabled(),
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                UncompressedStreamCompressionDecorator.INSTANCE,
                new CloseableRegistry());
    }

    @Test
    public void setupKeyedStateBackend() throws Exception {

        blocker = new OneShotLatch();
        waiter = new OneShotLatch();
        testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
        testStreamFactory.setBlockerLatch(blocker);
        testStreamFactory.setWaiterLatch(waiter);
        testStreamFactory.setAfterNumberInvocations(10);

        //        prepareRocksDB();

        keyedStateBackend =
                builderForTestDB(
                                TEMP_FOLDER
                                        .newFolder(), // this is not used anyways because the DB is
                                // injected
                                IntSerializer.INSTANCE)
                        .build();

        testState1 =
                keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("TestState-1", Integer.class, 0));

        testState2 =
                keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("TestState-2", String.class, ""));

        for (int i = 0; i < 100; ++i) {
            keyedStateBackend.setCurrentKey(i);
            testState1.update(4200 + i);
            testState2.update("S-" + (4200 + i));
        }
    }

    private void checkRemove(IncrementalRemoteKeyedStateHandle remove, SharedStateRegistry registry)
            throws Exception {
        for (StateHandleID id : remove.getSharedState().keySet()) {
            verify(registry, times(0))
                    .unregisterReference(remove.createSharedStateRegistryKeyFromFileName(id));
        }

        remove.discardState();

        for (StateHandleID id : remove.getSharedState().keySet()) {
            verify(registry)
                    .unregisterReference(remove.createSharedStateRegistryKeyFromFileName(id));
        }
    }

    private void runStateUpdates() throws Exception {
        for (int i = 50; i < 150; ++i) {
            if (i % 10 == 0) {
                Thread.sleep(1);
            }
            keyedStateBackend.setCurrentKey(i);
            testState1.update(4200 + i);
            testState2.update("S-" + (4200 + i));
        }
    }

    private static class AcceptAllFilter implements IOFileFilter {
        @Override
        public boolean accept(File file) {
            return true;
        }

        @Override
        public boolean accept(File file, String s) {
            return true;
        }
    }
}

//        allCreatedCloseables = new ArrayList<>();
//
//        doAnswer(
//                new Answer<Object>() {
//
//                    @Override
//                    public Object answer(InvocationOnMock invocationOnMock)
//                            throws Throwable {
//                        RocksIterator rocksIterator =
//                                spy((RocksIterator) invocationOnMock.callRealMethod());
//                        allCreatedCloseables.add(rocksIterator);
//                        return rocksIterator;
//                    }
//                })
//                .when(keyedStateBackend.db)
//                .newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));
//
//        doAnswer(
//                new Answer<Object>() {
//
//                    @Override
//                    public Object answer(InvocationOnMock invocationOnMock)
//                            throws Throwable {
//                        Snapshot snapshot =
//                                spy((Snapshot) invocationOnMock.callRealMethod());
//                        allCreatedCloseables.add(snapshot);
//                        return snapshot;
//                    }
//                })
//                .when(keyedStateBackend.db)
//                .getSnapshot();
//
//        doAnswer(
//                new Answer<Object>() {
//
//                    @Override
//                    public Object answer(InvocationOnMock invocationOnMock)
//                            throws Throwable {
//                        ColumnFamilyHandle snapshot =
//                                spy((ColumnFamilyHandle)
// invocationOnMock.callRealMethod());
//                        allCreatedCloseables.add(snapshot);
//                        return snapshot;
//                    }
//                })
//                .when(keyedStateBackend.db)
//                .createColumnFamily(any(ColumnFamilyDescriptor.class));

// small safety net for instance cleanups, so that no native objects are left
//    @After
//    public void cleanupRocksDB() {
//        if (keyedStateBackend != null) {
//            IOUtils.closeQuietly(keyedStateBackend);
//            keyedStateBackend.dispose();
//        }
//        IOUtils.closeQuietly(defaultCFHandle);
//        IOUtils.closeQuietly(db);
//        IOUtils.closeQuietly(optionsContainer);
//
//        if (allCreatedCloseables != null) {
//            for (RocksObject rocksCloseable : allCreatedCloseables) {
//                verify(rocksCloseable, times(1)).close();
//            }
//            allCreatedCloseables = null;
//        }
//    }

//    private void verifyRocksObjectsReleased() {
//        // Ensure every RocksObject was closed exactly once
//        for (RocksObject rocksCloseable : allCreatedCloseables) {
//            verify(rocksCloseable, times(1)).close();
//        }
//
//        assertNotNull(null, keyedStateBackend.db);
//        RocksDB spyDB = keyedStateBackend.db;
//
//        if (!enableIncrementalCheckpointing) {
//            verify(spyDB, times(1)).getSnapshot();
//            verify(spyDB, times(1)).releaseSnapshot(any(Snapshot.class));
//        }
//
//        keyedStateBackend.dispose();
//        verify(spyDB, times(1)).close();
//        assertEquals(true, keyedStateBackend.isDisposed());
//    }

//    @Test
//    public void testCancelRunningSnapshot() throws Exception {
//        setupRocksKeyedStateBackend();
//        try {
//            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
//                    keyedStateBackend.snapshot(
//                            0L,
//                            0L,
//                            testStreamFactory,
//                            CheckpointOptions.forCheckpointWithDefaultLocation());
//            Thread asyncSnapshotThread = new Thread(snapshot);
//            asyncSnapshotThread.start();
//            waiter.await(); // wait for snapshot to run
//            waiter.reset();
//            runStateUpdates();
//            snapshot.cancel(true);
//            blocker.trigger(); // allow checkpointing to start writing
//
//            for (BlockingCheckpointOutputStream stream :
// testStreamFactory.getAllCreatedStreams()) {
//                assertTrue(stream.isClosed());
//            }
//
//            waiter.await(); // wait for snapshot stream writing to run
//            try {
//                snapshot.get();
//                fail();
//            } catch (Exception ignored) {
//            }
//
//            asyncSnapshotThread.join();
//            verifyRocksObjectsReleased();
//        } finally {
//            this.keyedStateBackend.dispose();
//            this.keyedStateBackend = null;
//        }
//    }

//    @Test
//    public void testCompletingSnapshot() throws Exception {
//        setupRocksKeyedStateBackend();
//        try {
//            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
//                    keyedStateBackend.snapshot(
//                            0L,
//                            0L,
//                            testStreamFactory,
//                            CheckpointOptions.forCheckpointWithDefaultLocation());
//            Thread asyncSnapshotThread = new Thread(snapshot);
//            asyncSnapshotThread.start();
//            waiter.await(); // wait for snapshot to run
//            waiter.reset();
//            runStateUpdates();
//            blocker.trigger(); // allow checkpointing to start writing
//            waiter.await(); // wait for snapshot stream writing to run
//
//            SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
//            KeyedStateHandle keyedStateHandle = snapshotResult.getJobManagerOwnedSnapshot();
//            assertNotNull(keyedStateHandle);
//            assertTrue(keyedStateHandle.getStateSize() > 0);
//            assertEquals(2, keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups());
//
//            for (BlockingCheckpointOutputStream stream :
// testStreamFactory.getAllCreatedStreams()) {
//                assertTrue(stream.isClosed());
//            }
//
//            asyncSnapshotThread.join();
//            verifyRocksObjectsReleased();
//        } finally {
//            this.keyedStateBackend.dispose();
//            this.keyedStateBackend = null;
//        }
//    }
//  //    @Test
//    //    public void testSharedIncrementalStateDeRegistration() throws Exception {
//    //        if (enableIncrementalCheckpointing) {
//    //            CheckpointableKeyedStateBackend<Integer> backend =
//    //                    createKeyedBackend(IntSerializer.INSTANCE);
//    //            try {
//    //                ValueStateDescriptor<String> kvId =
//    //                        new ValueStateDescriptor<>("id", String.class, null);
//    //
//    //                kvId.initializeSerializerUnlessSet(new ExecutionConfig());
//    //
//    //                ValueState<String> state =
//    //                        backend.getPartitionedState(
//    //                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
//    // kvId);
//    //
//    //                Queue<IncrementalRemoteKeyedStateHandle> previousStateHandles = new
//    // LinkedList<>();
//    //                SharedStateRegistry sharedStateRegistry = spy(new SharedStateRegistry());
//    //                for (int checkpointId = 0; checkpointId < 3; ++checkpointId) {
//    //
//    //                    reset(sharedStateRegistry);
//    //
//    //                    backend.setCurrentKey(checkpointId);
//    //                    state.update("Hello-" + checkpointId);
//    //
//    //                    RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
//    //                            backend.snapshot(
//    //                                    checkpointId,
//    //                                    checkpointId,
//    //                                    createStreamFactory(),
//    //                                    CheckpointOptions.forCheckpointWithDefaultLocation());
//    //
//    //                    snapshot.run();
//    //
//    //                    SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
//    //
//    //                    IncrementalRemoteKeyedStateHandle stateHandle =
//    //                            (IncrementalRemoteKeyedStateHandle)
//    //                                    snapshotResult.getJobManagerOwnedSnapshot();
//    //
//    //                    Map<StateHandleID, StreamStateHandle> sharedState =
//    //                            new HashMap<>(stateHandle.getSharedState());
//    //
//    //                    stateHandle.registerSharedStates(sharedStateRegistry);
//    //
//    //                    for (Map.Entry<StateHandleID, StreamStateHandle> e :
//    // sharedState.entrySet()) {
//    //                        verify(sharedStateRegistry)
//    //                                .registerReference(
//    //
// stateHandle.createSharedStateRegistryKeyFromFileName(
//    //                                                e.getKey()),
//    //                                        e.getValue());
//    //                    }
//    //
//    //                    previousStateHandles.add(stateHandle);
//    //                    ((CheckpointListener) backend).notifyCheckpointComplete(checkpointId);
//    //
//    //                    // -----------------------------------------------------------------
//    //
//    //                    if (previousStateHandles.size() > 1) {
//    //                        checkRemove(previousStateHandles.remove(), sharedStateRegistry);
//    //                    }
//    //                }
//    //
//    //                while (!previousStateHandles.isEmpty()) {
//    //
//    //                    reset(sharedStateRegistry);
//    //
//    //                    checkRemove(previousStateHandles.remove(), sharedStateRegistry);
//    //                }
//    //            } finally {
//    //                IOUtils.closeQuietly(backend);
//    //                backend.dispose();
//    //            }
//    //        }
//    //    }
//

//    @Test
//    public void testCorrectMergeOperatorSet() throws Exception {
//        prepareRocksDB();
//        final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());
//        RocksDBKeyedStateBackend<Integer> test = null;
//
//        try {
//            test =
//                    RocksDBTestUtils.builderForTestDB(
//                            TEMP_FOLDER.newFolder(),
//                            IntSerializer.INSTANCE,
//                            db,
//                            defaultCFHandle,
//                            columnFamilyOptions)
//                            .setEnableIncrementalCheckpointing(enableIncrementalCheckpointing)
//                            .build();
//
//            ValueStateDescriptor<String> stubState1 =
//                    new ValueStateDescriptor<>("StubState-1", StringSerializer.INSTANCE);
//            test.createInternalState(StringSerializer.INSTANCE, stubState1);
//            ValueStateDescriptor<String> stubState2 =
//                    new ValueStateDescriptor<>("StubState-2", StringSerializer.INSTANCE);
//            test.createInternalState(StringSerializer.INSTANCE, stubState2);
//
//            // The default CF is pre-created so sum up to 2 times (once for each stub state)
//            verify(columnFamilyOptions, Mockito.times(2))
//                    .setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
//        } finally {
//            if (test != null) {
//                IOUtils.closeQuietly(test);
//                test.dispose();
//            }
//            columnFamilyOptions.close();
//        }
//    }

//    @Test
//    public void testReleasingSnapshotAfterBackendClosed() throws Exception {
//        setupRocksKeyedStateBackend();
//
//        try {
//            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
//                    keyedStateBackend.snapshot(
//                            0L,
//                            0L,
//                            testStreamFactory,
//                            CheckpointOptions.forCheckpointWithDefaultLocation());
//
//            RocksDB spyDB = keyedStateBackend.db;
//
//            if (!enableIncrementalCheckpointing) {
//                verify(spyDB, times(1)).getSnapshot();
//                verify(spyDB, times(0)).releaseSnapshot(any(Snapshot.class));
//            }
//
//            // Ensure every RocksObjects not closed yet
//            for (RocksObject rocksCloseable : allCreatedCloseables) {
//                verify(rocksCloseable, times(0)).close();
//            }
//
//            snapshot.cancel(true);
//
//            this.keyedStateBackend.dispose();
//
//            verify(spyDB, times(1)).close();
//            assertEquals(true, keyedStateBackend.isDisposed());
//
//            // Ensure every RocksObjects was closed exactly once
//            for (RocksObject rocksCloseable : allCreatedCloseables) {
//                verify(rocksCloseable, times(1)).close();
//            }
//
//        } finally {
//            keyedStateBackend.dispose();
//            keyedStateBackend = null;
//        }
//    }
//
//    @Test
//    public void testDismissingSnapshot() throws Exception {
//        setupRocksKeyedStateBackend();
//        try {
//            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
//                    keyedStateBackend.snapshot(
//                            0L,
//                            0L,
//                            testStreamFactory,
//                            CheckpointOptions.forCheckpointWithDefaultLocation());
//            snapshot.cancel(true);
//            verifyRocksObjectsReleased();
//        } finally {
//            this.keyedStateBackend.dispose();
//            this.keyedStateBackend = null;
//        }
//    }

//    @Test
//    public void testDismissingSnapshotNotRunnable() throws Exception {
//        setupRocksKeyedStateBackend();
//        try {
//            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
//                    keyedStateBackend.snapshot(
//                            0L,
//                            0L,
//                            testStreamFactory,
//                            CheckpointOptions.forCheckpointWithDefaultLocation());
//            snapshot.cancel(true);
//            Thread asyncSnapshotThread = new Thread(snapshot);
//            asyncSnapshotThread.start();
//            try {
//                snapshot.get();
//                fail();
//            } catch (Exception ignored) {
//
//            }
//            asyncSnapshotThread.join();
//            verifyRocksObjectsReleased();
//        } finally {
//            this.keyedStateBackend.dispose();
//            this.keyedStateBackend = null;
//        }
//    }
//

//    @Test
//    public void testDisposeDeletesAllDirectories() throws Exception {
//        CheckpointableKeyedStateBackend<Integer> backend =
//                createKeyedBackend(IntSerializer.INSTANCE);
//        Collection<File> allFilesInDbDir =
//                FileUtils.listFilesAndDirs(
//                        new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());
//        try {
//            ValueStateDescriptor<String> kvId =
//                    new ValueStateDescriptor<>("id", String.class, null);
//
//            kvId.initializeSerializerUnlessSet(new ExecutionConfig());
//
//            ValueState<String> state =
//                    backend.getPartitionedState(
//                            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
//
//            backend.setCurrentKey(1);
//            state.update("Hello");
//
//            // more than just the root directory
//            assertTrue(allFilesInDbDir.size() > 1);
//        } finally {
//            IOUtils.closeQuietly(backend);
//            backend.dispose();
//        }
//        allFilesInDbDir =
//                FileUtils.listFilesAndDirs(
//                        new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());
//
//        // just the root directory left
//        assertEquals(1, allFilesInDbDir.size());
//    }
//
