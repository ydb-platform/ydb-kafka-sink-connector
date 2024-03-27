package ydb.kafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.PrimitiveType;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;
import ydb.kafka.connector.utils.DataUtility;
import ydb.kafka.connector.utils.Record;

import java.text.MessageFormat;
import java.util.*;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.SINK_SERVER_CONNECTION_STRING;
import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.SOURCE_TOPIC;

@Slf4j
public class KafkaSinkTask extends SinkTask {
    private KafkaSinkConnectorConfig config;
    private String connectorName;
    private GrpcTransport transport;
    private final AuthProvider authProvider = NopAuthProvider.INSTANCE;
    private TableClient tableClient;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            log.info("Task config setting : " + props.toString());
            connectorName = props.get("name");
            config = KafkaSinkConnectorConfig.create(props);

            transport = GrpcTransport.forConnectionString(config.getString(SINK_SERVER_CONNECTION_STRING))
                    .withAuthProvider(authProvider).build();
            tableClient = TableClient.newClient(transport).build();
            createTable();
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }

    }

    public void createTable() {
        String sourceTopicName = config.getString(SOURCE_TOPIC);
        SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

        TableDescription seriesTable = TableDescription.newBuilder()
                .addNonnullColumn("offset", PrimitiveType.Int64)
                .addNonnullColumn("partition", PrimitiveType.Int32)
                .addNullableColumn("key", PrimitiveType.Text)
                .addNullableColumn("value", PrimitiveType.Text)
                .setPrimaryKeys("offset", "partition")
                .build();

        retryCtx.supplyStatus(session -> session.createTable(transport.getDatabase() + "/" + sourceTopicName, seriesTable))
                .join().expectSuccess("Can't create table /" + sourceTopicName);

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            if (record.value() != null) {
                try {
                    Record<String, String> parseRecord = DataUtility.createRecord(record);
                    saveRecord(parseRecord);
                } catch (Exception e) {
                    log.error(e.getMessage() + " / " + connectorName, e);
                }
            }
        }
    }

    private void saveRecord(Record<String, String> record) {
        String queryTemplate
                = "UPSERT INTO " + config.getString(KafkaSinkConnectorConfig.SOURCE_TOPIC) + " (partition, offset, key, value)"
                + " VALUES ({0}, {1}, \"{2}\", \"{3}\");";

        String query = MessageFormat.format(queryTemplate, record.partition, record.offset, record.key, record.value);

        SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

        TxControl<TxControl.TxSerializableRw> txControl = TxControl.serializableRw().setCommitTx(true);

        retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
                .join().getValue();

        log.info(query);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        transport.close();
        tableClient.close();
    }
}