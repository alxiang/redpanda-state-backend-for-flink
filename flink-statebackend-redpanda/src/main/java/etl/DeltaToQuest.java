package etl;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.postgresql.xa.PGXADataSource;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import io.delta.flink.source.DeltaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.sql.Timestamp;
import utils.KafkaRecord;

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
            .flatMap((RowData t, Collector<KafkaRecord> out) -> {
                KafkaRecord record = new KafkaRecord();
                record.key = t.getString(0).toString();
                record.value = t.getLong(1);
                out.collect(record);
            })
            .flatMap(new QuestDBInsertMap("vectortable", "10.10.1.1"))
            .addSink(new DiscardingSink());
        //     .addSink(JdbcSink.exactlyOnceSink(
        //         "insert into vectortable (word, count, ts) values (?,?,?)",
        //         (ps, t) -> {
        //             ps.setString(1, t.getString(0).toString());
        //             ps.setLong(2, t.getLong(1));
        //             ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        //         },
        //         JdbcExecutionOptions.builder().build(), // TODO: may want to tune these, e.g. batch size
        //         JdbcExactlyOnceOptions.defaults(),
        //         () -> {
        //             // create a driver-specific XA DataSource
        //             PGXADataSource ds = new PGXADataSource();
        //             ds.setUrl("jdbc:postgresql://localhost:8812/vectortable");
        //             ds.setUser("admin");
        //             ds.setPassword("quest");
        //             return ds;
        //         }
        // ));
        

		env.execute("Delta -> Flink -> Quest");
	}
}