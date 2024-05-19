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

    public static Record<byte[], byte[]> newInstance(SinkRecord sinkRecord) {
        return new Record<>(
                (byte[]) sinkRecord.key(),
                (byte[]) sinkRecord.value(),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset()
        );
    }
}
