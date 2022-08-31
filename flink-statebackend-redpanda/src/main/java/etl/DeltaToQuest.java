package etl;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.kafka.common.TopicPartition;
import org.postgresql.xa.PGXADataSource;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.source.DeltaSource;
import shapeless.DataT;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.hadoop.conf.Configuration;

import utils.KafkaRecord;
import utils.RowDataFlatMap;
import utils.TupleRecordDeserializationSchema;

import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DeltaToQuest {
      
    public static void main(String[] args) throws Exception {

        String directory_daemon_address = "127.0.0.1";
        Long checkpointing_interval = 1000L;
        Long num_producers = 1L;
        String application = "Wiki";
        String table_name = null;
        String topic = null;
        Long num_records = null;

        if(args.length >= 1){
			directory_daemon_address = args[0];
		}	
        if(args.length >= 2){
            checkpointing_interval = Long.valueOf(args[1]);
        }
        if(args.length >= 3){
            application = args[2];
        }
        if(args.length >= 4){
            num_producers = Long.valueOf(args[3]);
        }

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		env.disableOperatorChaining();
		env.enableCheckpointing(checkpointing_interval);

        if(application.equals("Wiki")){
            table_name = "wikitable";
            topic = "Wiki";
            num_records = 5436759L;
        }
        else if(application.equals("VectorSimDelta")){
            table_name = "vectortable";
            topic = "Vector";
            num_records = 100000L;
        }

        URI deltaTablePath = new URI("file:///tmp/delta");

        DeltaSource<RowData> source = DeltaSource
        .forBoundedRowData(
            new Path(deltaTablePath),
            new Configuration())
        .build();
            
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Delta Source")
            .addSink(JdbcSink.exactlyOnceSink(
                "insert into vectortable (word, count, ts) values (?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getString(1).toString());
                    ps.setLong(2, t.getLong(2));
                    ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                },
                JdbcExecutionOptions.builder().build(),
                JdbcExactlyOnceOptions.defaults(),
                () -> {
                    // create a driver-specific XA DataSource
                    PGXADataSource ds = new PGXADataSource();
                    ds.setUrl("jdbc:postgresql://localhost:8812/vectortable");
                    ds.setUser("admin");
                    ds.setPassword("quest");
                    return ds;
                }
        ));
        

		env.execute("Delta -> Flink -> Quest");
	}
}