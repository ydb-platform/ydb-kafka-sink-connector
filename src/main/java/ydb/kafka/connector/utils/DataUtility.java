package ydb.kafka.connector.utils;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class DataUtility {

    public static ByteBuffer parseValue(Schema schema, Object value) { // TODO TBD
        return null;
    }

    public static Record<String, String> createRecord(SinkRecord sinkRecord) {
        return new Record<>(
                sinkRecord.key().toString(),
                sinkRecord.value().toString(),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset()
        );
    }

}