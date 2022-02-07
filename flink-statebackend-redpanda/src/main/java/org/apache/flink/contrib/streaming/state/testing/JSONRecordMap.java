package org.apache.flink.contrib.streaming.state.testing;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.*;
import org.json.JSONObject;

import org.apache.flink.contrib.streaming.state.RedpandaValueState;

public class JSONRecordMap extends RichFlatMapFunction<String, String>{

    private transient ValueState<String> records;
    private String TOPIC;
    private boolean BATCH_WRITES = false;
    private String directory_daemon_address;

    public JSONRecordMap() {
    }

    public JSONRecordMap(String REDPANDA_TOPIC) {
        TOPIC = REDPANDA_TOPIC;
    }

    public JSONRecordMap(String REDPANDA_TOPIC, boolean BATCH_WRITES_, String directory_daemon_address_) {
        TOPIC = REDPANDA_TOPIC;
        BATCH_WRITES = BATCH_WRITES_;
        directory_daemon_address = directory_daemon_address_;
    }

    // https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/streaming/state.html
    @Override
    public void flatMap(String input, Collector<String> out) throws Exception {        
        // update the state
        JSONObject jsonObject = new JSONObject(input);
        String value = jsonObject.getString("payload");
        records.update(value); 
        out.collect(value);
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<String>(
                        "JSON Records", // the state name
                        TypeInformation.of(new TypeHint<String>() {})); // type information
        records = getRuntimeContext().getState(descriptor);

        try{
            if(TOPIC != null){
                ((RedpandaValueState) records).TOPIC = TOPIC;
                ((RedpandaValueState) records).BATCH_WRITES = BATCH_WRITES;
                ((RedpandaValueState) records).directory_daemon_address = directory_daemon_address;
                System.out.println("Re-set topic to: " + TOPIC);
                if(BATCH_WRITES){
                    System.out.println("Batching writes set");
                }
            }
        }
        catch (Exception e) {
            System.out.println("Can't reconfigure this backend.");
        }
    }
}
