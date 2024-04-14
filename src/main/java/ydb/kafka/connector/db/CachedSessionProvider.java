package ydb.kafka.connector.db;

import lombok.extern.slf4j.Slf4j;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;
import ydb.kafka.connector.security.SinkAuthProvider;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.YDB_CONNECTION_STRING;

@Slf4j
public class CachedSessionProvider implements AutoCloseable {
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private SessionRetryContext retryCtx;

    public CachedSessionProvider(KafkaSinkConnectorConfig config) {
        String connectionString = config.getString(YDB_CONNECTION_STRING);

        SinkAuthProvider authProvider = new SinkAuthProvider(config);

        log.info(authProvider.get().toString());

        this.transport =
                GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider.get())
                .build();

        this.tableClient = TableClient.newClient(transport).build();
        log.info("Cached session provider has been successfully created. Connection string: {}", connectionString);
    }

    public SessionRetryContext getSession() {
        if (this.retryCtx == null) {
            this.retryCtx = SessionRetryContext.create(tableClient).build();
        }

        return retryCtx;
    }

    public String getDatabase() {
        return transport.getDatabase();
    }

    @Override
    public void close() {
        transport.close();
    }
}
