package utils;

public class KafkaRecord {
    public String key;
    public Long value;
    public long offset;
    public int partition;
    public String topic;
    public long timestamp;    
}