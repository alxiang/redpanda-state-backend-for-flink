package org.apache.flink.contrib.streaming.state.query;

import jiffy.JiffyClient;

import org.apache.flink.contrib.streaming.state.utils.InetAddressLocalHostUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.*;

// QuestDB imports
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class QuestDBInsertMap extends RichFlatMapFunction<KafkaRecord, Long> implements CheckpointedFunction {

    private TableWriter writer;
    private CairoEngine engine;
    private SqlCompiler compiler;
    private String table_name = "wikitable";
    JiffyClient client;
    String directory_daemon_address;
    Long start_time;
    Long time_now;

    public QuestDBInsertMap(String table_name_, String directory_daemon_address_) {
        table_name = table_name_;
        directory_daemon_address = directory_daemon_address_;

    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Long snapshot_start_time = System.nanoTime();
        writer.commit();
        
        if(start_time == null){
            start_time = System.currentTimeMillis();
        }
        time_now = System.currentTimeMillis();
        Long snapshot_end_time = System.nanoTime();
        System.out.println("[SNAPSHOT_TIME]: " + (snapshot_end_time - snapshot_start_time));
        System.out.println("[FLINK_QUESTDB] Runtime: " + (time_now - start_time));
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {

    }       

    @Override
    public void flatMap(KafkaRecord record, Collector<Long> out) throws Exception {        
        String key = record.key;
        Long value = record.value;

        TableWriter.Row row = writer.newRow(record.timestamp);
        row.putStr(0, key);
        row.putLong(1, value);
        row.append();
    }

    @Override
    public void open(Configuration config) {

        // this logic with non-serializable data types needs to 
        // be in the open and close methods of the flatmap
        connectToJiffy();

        // Ask Jiffy for a memory mapped file for the parquet file
        allocateJiffyFile("/opt/flink/.questdb/"+table_name+"/default/count.d");
        allocateJiffyFile("/opt/flink/.questdb/"+table_name+"/default/ts.d");
        allocateJiffyFile("/opt/flink/.questdb/"+table_name+"/default/word.d");
        allocateJiffyFile("/opt/flink/.questdb/"+table_name+"/default/word.i");

        System.out.println("JiffyClient and Jiffy files for table successfully initialized.");

        // Set up questdb
        final CairoConfiguration configuration = new DefaultCairoConfiguration("/opt/flink/.questdb");

        // CairoEngine is a resource manager for embedded QuestDB
        this.engine = new CairoEngine(configuration);

        // Execution context is a conduit for passing SQL execution artefacts to the execution site
        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
        compiler = new SqlCompiler(engine);

        // drop the table if it exists and re-create it
        try {
            // this.compiler.compile("drop table "+table_name, ctx);
            this.compiler.compile("create table "+table_name+" (word string, count long, ts timestamp) timestamp(ts)", ctx);
        } catch (SqlException e) {
            e.printStackTrace();
        }

        // This TableWriter instance has an exclusive (intra and interprocess) lock on the table
        this.writer = engine.getWriter(ctx.getCairoSecurityContext(), table_name, "testing");
    }

    @Override
    public void close(){
        this.writer.close();
        System.out.println("[FLINK_QUESTDB] Runtime: " + (time_now - start_time));
    }

    private void connectToJiffy() {

        try {
            System.out.println("Trying to connect to Jiffy at address: " + directory_daemon_address);
            this.client = new JiffyClient(directory_daemon_address, 9090, 9091);
        } catch (Exception e) {
            System.out.println("Failed to connect to Jiffy with client, are the Jiffy directory and storage daemons running?");
            System.out.println(e);
            System.exit(-1);
        }
    }

    private void allocateJiffyFile(String filePath) {
        try {
            String hostAddress = InetAddressLocalHostUtil.getLocalHostAsString();
            System.out.println("Got the local host address as: " + hostAddress);
            System.out.println("Asking for Jiffy file at: " + filePath);

            client.createFile(filePath, "local://home", hostAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}




