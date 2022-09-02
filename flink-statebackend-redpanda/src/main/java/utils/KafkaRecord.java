package utils;

public class KafkaRecord {
    public String key;
    public Long value;
    public long offset;
    public int partition;
    public String topic;
    public long timestamp;   

    public KafkaRecord(){} 
    
    public KafkaRecord(String key_, Long value_){
        this.key = key_;
        this.value = value_;
    }
}
