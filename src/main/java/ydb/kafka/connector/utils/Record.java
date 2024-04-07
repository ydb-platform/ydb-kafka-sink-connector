package ydb.kafka.connector.utils;

import org.apache.kafka.connect.sink.SinkRecord;

public class Record<K, V> {
    public K key;
    public V value;
    public int partition;
    public long offset;

    public Record(K key, V value, int partition, long offset) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
    }

    public static Record<String, String> newInstance(SinkRecord sinkRecord) {
        return new Record<>(
                sinkRecord.key().toString(),
                sinkRecord.value().toString(),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset()
        );
    }
}
