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

public class QueryKafkaFetcher<T> extends KafkaFetcher<T> {
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
    }

    @Override
    public HashMap<KafkaTopicPartition, Long> snapshotCurrentState() {
        HashMap<KafkaTopicPartition, Long> state = super.snapshotCurrentState();
        
        // Implement QuestDB checkpoint logic

        return state;
    }
}
