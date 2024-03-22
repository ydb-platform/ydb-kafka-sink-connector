package ydb.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.PrimitiveType;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.*;

public class KafkaSinkConnector extends SinkConnector {
    private final static Logger log = LoggerFactory.getLogger(KafkaSinkConnector.class);

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            log.info("Connector props : " + props.toString());
            log.info("Creating table");
            createTable();
            new KafkaSinkConnectorConfig(props);

        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    public void createTable() { // разумно ли сразу создавать таблицу (у постгреса таблица создается при первой записи, а не при подключении)
        // TODO включить автосоздание в зависимости от конфига
        String sourceTopicName = configProperties.get(SOURCE_TOPIC); // TODO сделать несколько топиков
        AuthProvider authProvider = NopAuthProvider.INSTANCE;

        try (GrpcTransport transport = GrpcTransport.forConnectionString(SINK_SERVER_CONNECTION_STRING)
                        .withAuthProvider(authProvider) // Or this method could not be called at all
                        .build()) {
            try (TableClient tableClient = TableClient.newClient(transport).build()) {
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
        }

    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put("task.id", Integer.toString(i));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        return KafkaSinkConnectorConfig.CONFIG;
    }

    @Override
    public void stop() {
    }
}

