package ydb.kafka.connector.utils;

import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.*;

@Slf4j
public class BufferedRecords {

    private final SessionRetryContext retryCtx;
    private List<Record<String, String>> records = new ArrayList<>();
    private final int batchSize;
    private final String database;
    private final String destinationTable;

    public BufferedRecords(
            int batchSize,
            String database,
            String destinationTable,
            SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
        this.batchSize = batchSize;
        this.database = database;
        this.destinationTable = destinationTable;
    }

    public List<Record<String, String>> add(SinkRecord record) {
        final List<Record<String, String>> flushed = new ArrayList<>();

        records.add(Record.newInstance(record));

        if (records.size() >= batchSize) {
            long start = System.currentTimeMillis();
            flushed.addAll(flush());
            long end = System.currentTimeMillis();
            log.info("The flush of batch size {} took {} ms", batchSize, (end - start));
        }
        return flushed;
    }

    public List<Record<String, String>> flush() {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());

        executeUpdates()
                .expectSuccess("Bulk upsert problem");

        final List<Record<String, String>> flushedRecords = records;
        records = new ArrayList<>();

        return flushedRecords;
    }

    private Status executeUpdates() {
        StructType seriesType = StructType.of(
                Map.of(
                        "topic", PrimitiveType.Text,
                        "offset", PrimitiveType.Int64,
                        "partition", PrimitiveType.Int32,
                        "key", PrimitiveType.Text,
                        "value", PrimitiveType.Text,
                        "timestamp", PrimitiveType.Timestamp
                )
        );

        ListValue seriesData = ListType.of(seriesType).newValue(
                records.stream().map(record ->
                        seriesType.newValue(
                                Map.of(
                                        "topic", PrimitiveValue.newText(record.topic()),
                                        "offset", PrimitiveValue.newInt64(record.offset()),
                                        "partition", PrimitiveValue.newInt32(record.partition()),
                                        "key", PrimitiveValue.newText(record.key()),
                                        "value", PrimitiveValue.newText(record.value()),
                                        "timestamp", PrimitiveValue.newTimestamp(record.timestamp())
                                )
                        )).collect(Collectors.toList())
        );

        return retryCtx.supplyStatus(
                        session -> session.executeBulkUpsert(
                                database + "/" + destinationTable,
                                seriesData,
                                new BulkUpsertSettings()
                        ))
                .join();
    }
}