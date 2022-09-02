package input;

import io.delta.flink.sink.DeltaSink;
import shadedelta.org.apache.parquet.format.LogicalType;
import utils.KafkaRecord;
import utils.RowDataFlatMap;
import utils.TupleRecordDeserializationSchema;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaToDelta {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();
        env.enableCheckpointing(100L);

        String topic = "Vector";

        // Ingest data from Kafka
        HashMap<TopicPartition, Long> partition_map = new HashMap<TopicPartition,Long>();
        partition_map.put(
            new TopicPartition(topic, 0), 
            100000L
        );

        KafkaSource<KafkaRecord> source = KafkaSource
            .<KafkaRecord>builder()
            .setBootstrapServers("10.10.1.1:9192")
            .setGroupId("DeltaTablePopulator")
            .setTopics(topic)
            .setDeserializer(new TupleRecordDeserializationSchema())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.offsets(partition_map))
            .build();

        // Sink data into Delta lake table
        URI deltaTablePath = new URI("file:///tmp/delta");

        RowField key = new RowField("key", DataTypes.STRING().getLogicalType());
        RowField value = new RowField("value", DataTypes.BIGINT().getLogicalType());
        List<RowField> fields = new ArrayList<RowField>();
        fields.add(key);
        fields.add(value);
        RowType rowType = new RowType(fields);

        DeltaSink<RowData> deltaSink = DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                new Configuration(),
                rowType)
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .flatMap(new RowDataFlatMap())
            .sinkTo(deltaSink);

        env.execute("Kafka -> Delta");
    }    
}
