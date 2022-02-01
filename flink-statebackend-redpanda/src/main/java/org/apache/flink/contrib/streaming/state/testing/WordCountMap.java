package org.apache.flink.contrib.streaming.state.testing;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.*;

public class WordCountMap extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>{

    private transient ValueState<Long> count;

    // https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/streaming/state.html
    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {

        // access state value
        Long currentCount = count.value();

        // update count
        currentCount = currentCount == null ? 1L : currentCount + 1L;
        // currentCount += 1; I commented out this extra +1 since I wasn't sure why we had it
        
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
    }
}
