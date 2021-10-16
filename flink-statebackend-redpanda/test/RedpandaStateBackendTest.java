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

/** Tests {@link RedpandaStateBackendTest}. */
@RunWith(Parameterized.class)
public class RedpandaStateBackendTest extends TestLogger {

    private OneShotLatch blocker;
    private OneShotLatch waiter;
    private BlockerCheckpointStreamFactory testStreamFactory;
    private RedpandaKeyedStateBackend<Integer> keyedStateBackend;
    // private ValueState<Integer> testState1;
    // private ValueState<String> testState2;

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

    protected RedpandaStateBackend getStateBackend() throws IOException {
        dbPath = TEMP_FOLDER.newFolder().getAbsolutePath();
        RedpandaStateBackend backend = new RedpandaStateBackend();
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

    public static <K> RedpandaKeyedStateBackendBuilder<K> builderForTestDB(
            File instanceBasePath, TypeSerializer<K> keySerializer) {
        return new RedpandaKeyedStateBackendBuilder<>(
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
    }

    // private void checkRemove(IncrementalRemoteKeyedStateHandle remove, SharedStateRegistry registry)
    //         throws Exception {
    //     for (StateHandleID id : remove.getSharedState().keySet()) {
    //         verify(registry, times(0))
    //                 .unregisterReference(remove.createSharedStateRegistryKeyFromFileName(id));
    //     }

    //     remove.discardState();

    //     for (StateHandleID id : remove.getSharedState().keySet()) {
    //         verify(registry)
    //                 .unregisterReference(remove.createSharedStateRegistryKeyFromFileName(id));
    //     }
    // }

    // private void runStateUpdates() throws Exception {
    //     for (int i = 50; i < 150; ++i) {
    //         if (i % 10 == 0) {
    //             Thread.sleep(1);
    //         }
    //         keyedStateBackend.setCurrentKey(i);
    //         testState1.update(4200 + i);
    //         testState2.update("S-" + (4200 + i));
    //     }
    // }

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