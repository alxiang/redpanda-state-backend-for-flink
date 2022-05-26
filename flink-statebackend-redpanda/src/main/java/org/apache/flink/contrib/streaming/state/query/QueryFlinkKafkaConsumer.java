package org.apache.flink.contrib.streaming.state.query;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.List;

public class QueryFlinkKafkaConsumer<T> extends FlinkKafkaConsumer<T> {

    String table_name;

    public QueryFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
    }

    public QueryFlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props, String table_name_) {
        super(topic, valueDeserializer, props);
        table_name = table_name_;
    }

    public QueryFlinkKafkaConsumer(
            String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
        super(Collections.singletonList(topic), deserializer, props);
    }

    public QueryFlinkKafkaConsumer(
            List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
        super(topics, new KafkaDeserializationSchemaWrapper<>(deserializer), props);
    }


    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics)
            throws Exception {

        // make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        adjustAutoCommitConfig(properties, offsetCommitMode);

        return new QueryKafkaFetcher<>(
                sourceContext,
                assignedPartitionsWithInitialOffsets,
                watermarkStrategy,
                runtimeContext.getProcessingTimeService(),
                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                runtimeContext.getUserCodeClassLoader(),
                runtimeContext.getTaskNameWithSubtasks(),
                deserializer,
                properties,
                pollTimeout,
                runtimeContext.getMetricGroup(),
                consumerMetricGroup,
                useMetrics,
                table_name
                );
    }
}
