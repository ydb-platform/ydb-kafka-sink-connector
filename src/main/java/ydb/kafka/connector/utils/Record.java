package ydb.kafka.connector.utils;

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
}
