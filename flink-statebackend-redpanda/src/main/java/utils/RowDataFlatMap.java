package utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

public class RowDataFlatMap extends RichFlatMapFunction<KafkaRecord, RowData> {
    public RowDataFlatMap(){}

    @Override
    public void flatMap(KafkaRecord record, Collector<RowData> out) throws Exception {
        GenericRowData rowData = new GenericRowData(2);

        rowData.setField(0, record.key);
        rowData.setField(1, record.value);
        
        out.collect(rowData);
    }
}
