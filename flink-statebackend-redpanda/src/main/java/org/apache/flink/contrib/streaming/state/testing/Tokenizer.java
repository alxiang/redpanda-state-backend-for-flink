package org.apache.flink.contrib.streaming.state.testing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

// FlatMapFunction that tokenizes a String by whitespace characters and emits all String tokens.
public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
        for (String token : value.split("\\s+")) {
            
            String processed = token.replaceAll("\\p{Punct}", "").toLowerCase();

            if(processed.length() > 0){
                out.collect(
                    new Tuple2<String, Long>(
                        processed, 0L
                    )
                );
            }
        }
    }
  }