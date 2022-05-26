package org.apache.flink.contrib.streaming.state.query;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
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

import org.apache.flink.contrib.streaming.state.query.KafkaRecord;

public class QuestDBInsertMap extends RichFlatMapFunction<KafkaRecord, Long>{

    private TableWriter writer;
    private CairoEngine engine;
    private SqlCompiler compiler;
    private String table_name = "wikitable";

    public QuestDBInsertMap(String table_name_) {
        table_name = table_name_;
    }

    @Override
    public void flatMap(KafkaRecord record, Collector<Long> out) throws Exception {        
        String key = record.key;
        Long value = record.value;

        TableWriter.Row row = writer.newRow(record.timestamp);
        row.putStr(0, key);
        row.putLong(1, value);
        row.append();
        writer.commit();
    }

    @Override
    public void open(Configuration config) {

        // todo: create jiffy files for table here

        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<String>(
                        "QuestDB Records", // the state name
                        TypeInformation.of(new TypeHint<String>() {})); // type information

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
    }
}




