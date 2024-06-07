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
    private List<Record<byte[], byte[]>> records = new ArrayList<>();
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

    public List<Record<byte[], byte[]>> add(SinkRecord record) {
        final List<Record<byte[], byte[]>> flushed = new ArrayList<>();

        records.add(Record.newInstance(record));

        if (records.size() >= batchSize) {
            long start = System.currentTimeMillis();
            flushed.addAll(flush());
            long end = System.currentTimeMillis();
            log.info("The flush of batch size {} took {} ms", batchSize, (end - start));
        }
        return flushed;
    }

    public List<Record<byte[], byte[]>> flush() {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());

        executeUpdates()
                .expectSuccess("Bulk upsert problem");

        final List<Record<byte[], byte[]>> flushedRecords = records;
        records = new ArrayList<>();

        return flushedRecords;
    }

    private Status executeUpdates() {
        StructType seriesType = StructType.of(
                "offset", PrimitiveType.Int64,
                "partition", PrimitiveType.Int32,
                "key", PrimitiveType.Bytes,
                "value", PrimitiveType.Bytes
        );

        ListValue seriesData = ListType.of(seriesType).newValue(
                records.stream().map(record ->
                        seriesType.newValue(
                                "offset", PrimitiveValue.newInt64(record.offset),
                                "partition", PrimitiveValue.newInt32(record.partition),
                                "key", PrimitiveValue.newBytes(record.key != null ? record.key : new byte[0]),
                                "value", PrimitiveValue.newBytes(record.value != null ? record.value : new byte[0])
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