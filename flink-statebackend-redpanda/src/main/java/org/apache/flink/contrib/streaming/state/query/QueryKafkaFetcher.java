package org.apache.flink.contrib.streaming.state.query;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// QuestDB imports
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.Properties;

public class QueryKafkaFetcher<T> extends KafkaFetcher<T> {

    private CairoEngine engine;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl ctx;
    private String table_name = "wikitable";

    public QueryKafkaFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            String taskNameWithSubtasks,
            KafkaDeserializationSchema<T> deserializer,
            Properties kafkaProperties,
            long pollTimeout,
            MetricGroup subtaskMetricGroup,
            MetricGroup consumerMetricGroup,
            boolean useMetrics)
            throws Exception {
        super(
                sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarkStrategy,
                processingTimeProvider,
                autoWatermarkInterval,
                userCodeClassLoader,
                taskNameWithSubtasks,
                deserializer,
                kafkaProperties,
                pollTimeout,
                subtaskMetricGroup,
                consumerMetricGroup,
                useMetrics);

        final CairoConfiguration configuration = new DefaultCairoConfiguration("/home/alec/.questdb");

        // CairoEngine is a resource manager for embedded QuestDB
        this.engine = new CairoEngine(configuration);

        // Execution context is a conduit for passing SQL execution artefacts to the execution site
        ctx = new SqlExecutionContextImpl(engine, 1);
        compiler = new SqlCompiler(engine);

        try {
            // Replace live with staging
            this.compiler.compile(
                "create table "+table_name+"_prod (word string, count long, ts timestamp) timestamp(ts)", 
                ctx
            );
        } catch (SqlException e) {
            e.printStackTrace();
        }
    }

    @Override
    public final HashMap<KafkaTopicPartition, Long> snapshotCurrentState() throws RuntimeException {
        HashMap<KafkaTopicPartition, Long> state = super.snapshotCurrentState();
        
        /*
        Implement QuestDB checkpoint logic
        1. QuestDBInsertMap is continuously committing records into 'wikitable'
        2. At this checkpoint, we should copy 'wikitable' over to 'wikitable_prod'
        */

        // drop the table if it exists and re-create it
        try {
            // Replace live with staging
            this.compiler.compile(
                "BEGIN TRAN\n"+
                "DROP TABLE wikitable_prod;\n"+
                "EXEC sp_rename wikitable, wikitable_prod;\n"+
                "COMMIT", 
                ctx
            );
        } catch (SqlException e) {
            e.printStackTrace();
        }
        return state;
    }
}
