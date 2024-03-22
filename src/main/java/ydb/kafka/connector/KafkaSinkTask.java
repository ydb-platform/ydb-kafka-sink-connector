package ydb.kafka.connector;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.transaction.TxControl;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;
import ydb.kafka.connector.utils.DataUtility;
import ydb.kafka.connector.utils.Record;

import java.text.MessageFormat;
import java.util.*;

public class KafkaSinkTask extends SinkTask {
    private final static Logger log = LoggerFactory.getLogger(KafkaSinkTask.class);
    private String connectorName;
    private String connectionString;
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
            KafkaSinkConnectorConfig config;
            connectorName = props.get("name");
            config = new KafkaSinkConnectorConfig(props);

            connectionString = KafkaSinkConnectorConfig.SINK_SERVER_CONNECTION_STRING;
            transport = GrpcTransport.forConnectionString(connectionString).withAuthProvider(authProvider).build();
            tableClient = TableClient.newClient(transport).build();

        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }

    }

    @Override
    public void put(Collection<SinkRecord> records) { // note: полезно посмотреть на чужие реализации
        for (SinkRecord record : records) {
            if (record.value() != null) {
                try {
                    Record<String, String> parseRecord = DataUtility.createRecord(record); // TODO парсить в зависимости от схемы
                    saveRecord(parseRecord);
                } catch (Exception e) {
                    log.error(e.getMessage() + " / " + connectorName, e);
                }
            }
        }

    }

    private void saveRecord(Record<String, String> record) { // TODO разобраться с другими типами данных
        String queryTemplate
                = "UPSERT INTO mytopic (partition, offset, key, value) " // TODO хардкод топика
                + "VALUES ({0}, {1}, \"{2}\", \"{3}\");";

        String query = MessageFormat.format(queryTemplate, record.partition, record.offset, record.key, record.value);

        SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

        // Begin new transaction with SerializableRW mode
        TxControl txControl = TxControl.serializableRw().setCommitTx(true);

        // Executes data query with specified transaction control settings.
        retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
                .join().getValue();

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