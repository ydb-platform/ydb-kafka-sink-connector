package ydb.kafka.connector.db;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.PrimitiveType;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;
import ydb.kafka.connector.utils.BufferedRecords;

import java.util.Collection;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.*;

@Slf4j
public class YdbWriter implements AutoCloseable {
    private final KafkaSinkConnectorConfig config;
    private final CachedSessionProvider sessionProvider;

    public YdbWriter(KafkaSinkConnectorConfig config) {
        this.config = config;
        this.sessionProvider = new CachedSessionProvider(config);
        createTable();
    }


    public void write(final Collection<SinkRecord> records) {
        final SessionRetryContext sessionRetryContext = sessionProvider.getSession();
        final BufferedRecords buffer = new BufferedRecords(
                config.getInt(BATCH_SIZE),
                sessionProvider.getDatabase(),
                config.getString(DESTINATION_TABLE_NAME),
                sessionRetryContext);

        log.debug("Received {} records to write", records.size());
        records.forEach(buffer::add);

        buffer.flush();
    }

    @Override
    public void close() {
        sessionProvider.close();
    }

    private void createTable() {
        String destinationTableName = config.getString(DESTINATION_TABLE_NAME);
        String destinationDbName = sessionProvider.getDatabase();
        SessionRetryContext retryCtx = sessionProvider.getSession();

        TableDescription seriesTable = TableDescription.newBuilder()
                .addNonnullColumn("topic", PrimitiveType.Text)
                .addNonnullColumn("offset", PrimitiveType.Int64)
                .addNonnullColumn("partition", PrimitiveType.Int32)
                .addNullableColumn("key", PrimitiveType.Text)
                .addNullableColumn("value", PrimitiveType.Text)
                .addNonnullColumn("timestamp", PrimitiveType.Timestamp)
                .setPrimaryKeys("topic", "offset", "partition")
                .build();

        retryCtx.supplyStatus(session -> session.createTable(destinationDbName + "/" + destinationTableName, seriesTable))
                .join().expectSuccess("Can't create table /" + destinationTableName);

    }
}
