package ydb.kafka.connector.db;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.SINK_SERVER_CONNECTION_STRING;

public class CachedSessionProvider implements AutoCloseable {
    private final GrpcTransport transport;
    private final AuthProvider authProvider = NopAuthProvider.INSTANCE;
    private final TableClient tableClient;
    private SessionRetryContext retryCtx;


    public CachedSessionProvider(KafkaSinkConnectorConfig config) {
        this.transport = GrpcTransport
                .forConnectionString(config.getString(SINK_SERVER_CONNECTION_STRING))
                .withAuthProvider(authProvider)
                .build();
        this.tableClient = TableClient.newClient(transport).build();
    }

    public SessionRetryContext getSession() {
        if (this.retryCtx == null) {
            this.retryCtx = SessionRetryContext.create(tableClient).build();
        }

        return retryCtx;
    }

    @Override
    public void close() {
        transport.close();
    }
}
