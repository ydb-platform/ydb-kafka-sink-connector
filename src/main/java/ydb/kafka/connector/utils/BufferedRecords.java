package ydb.kafka.connector.utils;

import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.*;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.*;

@Slf4j
public class BufferedRecords {

    private final SessionRetryContext retryCtx;
    private List<Record<String, String>> records = new ArrayList<>();
    private final int batchSize;
    private final String database;
    private final String destinationTable;

    public BufferedRecords(
            KafkaSinkConnectorConfig config,
            SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
        this.batchSize = config.getInt(BATCH_SIZE);
        this.database = config.getString(YDB_DATABASE);
        this.destinationTable = config.getString(SOURCE_TOPIC);
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
                "offset", PrimitiveType.Int64,
                "partition", PrimitiveType.Int32,
                "key", PrimitiveType.Text,
                "value", PrimitiveType.Text
        );

        ListValue seriesData = ListType.of(seriesType).newValue(
                records.stream().map(record ->
                        seriesType.newValue(
                                "offset", PrimitiveValue.newInt64(record.offset),
                                "partition", PrimitiveValue.newInt32(record.partition),
                                "key", PrimitiveValue.newText(record.key),
                                "value", PrimitiveValue.newText(record.value)
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