package etl;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicPartition;
import org.postgresql.xa.PGXADataSource;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import utils.KafkaRecord;
import utils.TupleRecordDeserializationSchema;

import java.sql.Timestamp;
import java.util.HashMap;

public class KafkaToQuest {
      
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
        else if(application.equals("VectorSimKafka")){
            table_name = "vectortable";
            topic = "Vector";
            num_records = 100000L;
        }
        

        HashMap<TopicPartition, Long> partition_map = new HashMap<TopicPartition,Long>();
        partition_map.put(
            new TopicPartition(topic, 0), 
            num_records*num_producers
        );

        KafkaSource<KafkaRecord> source = KafkaSource
            .<KafkaRecord>builder()
            .setBootstrapServers(directory_daemon_address+":9192")
            .setGroupId("QuestDBConsumerFlink")
            .setTopics(topic)
            .setDeserializer(new TupleRecordDeserializationSchema())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setBounded(OffsetsInitializer.offsets(partition_map))
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")    
            .flatMap(new QuestDBInsertMap("vectortable", "10.10.1.1"))
            .addSink(new DiscardingSink());
        // .addSink(
        //     JdbcSink.sink(
        //         "insert into vectortable (word, count, ts) values (?,?,?)",
        //         (ps, t) -> {
        //             ps.setString(1, t.key);
        //             ps.setLong(2, t.value);
        //             ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        //         },
        //         JdbcExecutionOptions.builder().build(),
        //         new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        //             .withUrl("jdbc:postgresql://localhost:8812/vectortable")
        //             .withDriverName("org.postgresql.Driver")
        //             .withUsername("admin")
        //             .withPassword("quest")
        //             .build()
        //     )
        // );
        // .addSink(JdbcSink.exactlyOnceSink(
        //     "insert into vectortable (word, count, ts) values (?,?,?)",
        //     (ps, t) -> {
        //         ps.setString(1, t.key);
        //         ps.setLong(2, t.value);
        //         ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
        //     },
        //     JdbcExecutionOptions.builder().build(),
        //     JdbcExactlyOnceOptions.defaults(),
        //     () -> {
        //         // create a driver-specific XA DataSource
        //         PGXADataSource ds = new PGXADataSource();
        //         ds.setUrl("jdbc:postgresql://localhost:8812/vectortable");
        //         ds.setUser("admin");
        //         ds.setPassword("quest");
        //         return ds;
        //     }
        // ));

		env.execute("Kafka -> Flink -> Quest");
	}
}
