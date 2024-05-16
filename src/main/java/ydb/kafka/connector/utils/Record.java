package ydb.kafka.connector.utils;

import org.apache.kafka.connect.sink.SinkRecord;

public record Record<K, V>(K key, V value, int partition, long offset, long timestamp) {
    public static Record<String, String> newInstance(SinkRecord sinkRecord) {
        return new Record<>(
                sinkRecord.key().toString(),
                sinkRecord.value().toString(),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset(),
                sinkRecord.timestamp()
        );
    }
}
