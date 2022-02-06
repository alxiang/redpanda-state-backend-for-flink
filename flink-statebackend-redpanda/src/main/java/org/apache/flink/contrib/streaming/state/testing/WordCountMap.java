package org.apache.flink.contrib.streaming.state.testing;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.*;

import org.apache.flink.contrib.streaming.state.RedpandaValueState;

public class WordCountMap extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>{

    private transient ValueState<Long> count;
    private String TOPIC;
    private boolean BATCH_WRITES = false;
    private String directory_daemon_address;

    public WordCountMap() {
    }

    public WordCountMap(String REDPANDA_TOPIC) {
        TOPIC = REDPANDA_TOPIC;
    }

    public WordCountMap(String REDPANDA_TOPIC, boolean BATCH_WRITES_, String directory_daemon_address_) {
        TOPIC = REDPANDA_TOPIC;
        BATCH_WRITES = BATCH_WRITES_;
        directory_daemon_address = directory_daemon_address_;
    }

    // https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/streaming/state.html
    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {

        // access state value
        Long currentCount = count.value();

        // update count
        currentCount = currentCount == null ? 1L : currentCount + 1L;
        
        // update the state
        count.update(currentCount);

        // for debugging
        // Thread.sleep(1);
        // count.value();
        
        out.collect(new Tuple2<String, Long>(input.f0, currentCount));
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "Word counter", // the state name
                        TypeInformation.of(new TypeHint<Long>() {})); // type information
        count = getRuntimeContext().getState(descriptor);

        try{
            if(TOPIC != null){
                ((RedpandaValueState) count).TOPIC = TOPIC;
                ((RedpandaValueState) count).BATCH_WRITES = BATCH_WRITES;
                ((RedpandaValueState) count).directory_daemon_address = directory_daemon_address;
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
