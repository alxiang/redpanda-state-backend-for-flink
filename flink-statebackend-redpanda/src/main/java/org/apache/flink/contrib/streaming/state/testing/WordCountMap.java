package org.apache.flink.contrib.streaming.state.testing;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.*;

public class WordCountMap extends RichFlatMapFunction<String, Tuple2<String, Long>>{

    private transient ValueState<Long> count;

    // https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/streaming/state.html
    @Override
    public void flatMap(String input, Collector<Tuple2<String, Long>> out) throws Exception {

        // access state value
        Long currentCount = count.value();

        // update count
        currentCount += 1;

        // update the state
        count.update(currentCount);

        out.collect(new Tuple2<>(input, currentCount));

        if (currentCount >= 1) {
            out.collect(new Tuple2<>(input, currentCount));
            // count.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>(
                        "Word counter", // the state name
                        TypeInformation.of(new TypeHint<Long>() {}), // type information
                        0L); // default value of the state, if nothing was set
        count = getRuntimeContext().getState(descriptor);
    }
}
