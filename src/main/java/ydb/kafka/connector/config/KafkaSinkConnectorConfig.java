package ydb.kafka.connector.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

@Slf4j
public class KafkaSinkConnectorConfig extends AbstractConfig {

    public static final String SOURCE_TOPIC = "topics";
    public static final String SOURCE_TOPIC_DEFAULT_VALUE = "mytopic";
    public static final String SOURCE_TOPIC_DOC = "Define source topic";

    public static final String SINK_BOOTSTRAP_SERVER = "kafka.sink.bootstrap";
    public static final String SINK_BOOTSTRAP_SERVER_DEFAULT_VALUE = "localhost:9092";
    public static final String SINK_BOOTSTRAP_SERVER_DOC = "Define sink bootstrap";


    public static final String YDB_HOSTNAME = "ydb.host";
    public static final String YDB_HOSTNAME_DEFAULT_VALUE = "localhost";
    public static final String YDB_HOSTNAME_DOC = "Define YDB server hostname";

    public static final String YDB_GRPC_TLS_ENABLED = "ydb.grpc.tls.enabled";
    public static final boolean YDB_GRPC_TLS_ENABLED_DEFAULT_VALUE = false;
    public static final String YDB_GRPC_TLS_ENABLED_DOC = "If YDB_GRPC_TLS_ENABLED_DOC is true, then the gRPCs protocol is used, otherwise gRPC";

    public static final String YDB_GRPC_PORT = "ydb.grpc.port";
    public static final Integer YDB_GRPC_PORT_DEFAULT_VALUE = 2136;
    public static final String YDB_GRPC_PORT_DOC = "Define YDB server grpc port";

    public static final String YDB_AUTH_PROVIDER_TYPE = "ydb.auth.provider.type";
    public static final String YDB_AUTH_PROVIDER_TYPE_DEFAULT_VALUE = null;
    public static final String YDB_AUTH_PROVIDER_TYPE_DOC = "Define YDB auth type";

    public static final String YDB_AUTH_ACCESS_TOKEN = "ydb.auth.provider.access.token";
    public static final String YDB_AUTH_ACCESS_TOKEN_DEFAULT_VALUE = null;
    public static final String YDB_AUTH_ACCESS_TOKEN_DOC = "Define YDB access token";

    public static final String YDB_AUTH_SA_KEY_FILE = "ydb.auth.provider.sa.key.file";
    public static final String YDB_AUTH_SA_KEY_FILE_DEFAULT_VALUE = null;
    public static final String YDB_AUTH_SA_KEY_FILE_DOC = "Define YDB service account file";

    public static final String YDB_AUTH_USERNAME = "ydb.auth.provider.username";
    public static final String YDB_AUTH_USERNAME_DEFAULT_VALUE = null;
    public static final String YDB_AUTH_USERNAME_DOC = "Define YDB username";

    public static final String YDB_AUTH_PASSWORD = "ydb.auth.provider.password";
    public static final String YDB_AUTH_PASSWORD_DEFAULT_VALUE = null;
    public static final String YDB_AUTH_PASSWORD_DOC = "Define YDB auth type";

    public static final String YDB_DATABASE = "ydb.database";
    public static final String YDB_DATABASE_DEFAULT_VALUE = null;
    public static final String YDB_DATABASE_DOC = "Define YDB sink database name";

    public static final String YDB_CONNECTION_STRING = "ydb.connection.string";
    public static final String YDB_CONNECTION_STRING_DEFAULT_VALUE = null;
    public static final String YDB_CONNECTION_STRING_DOC = "YDB connection string";

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT_VALUE = 1000;
    private static final String BATCH_SIZE_DOC =
            "Specifies how many records to attempt to batch together for insertion into the destination"
                    + " table, when possible.";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT_VALUE = 10;
    private static final String MAX_RETRIES_DOC =
            "The maximum number of times to retry on errors before failing the task.";


    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final long RETRY_BACKOFF_MS_DEFAULT_VALUE = 500L;
    private static final String RETRY_BACKOFF_MS_DOC =
            "The time in milliseconds to wait following an error before a retry attempt is made.";

    public static ConfigDef CONFIG = new ConfigDef()
            .define(SOURCE_TOPIC, Type.STRING, SOURCE_TOPIC_DEFAULT_VALUE, Importance.HIGH, SOURCE_TOPIC_DOC)
            .define(SINK_BOOTSTRAP_SERVER, Type.STRING, SINK_BOOTSTRAP_SERVER_DEFAULT_VALUE, Importance.HIGH, SINK_BOOTSTRAP_SERVER_DOC)
            .define(YDB_HOSTNAME, Type.STRING, YDB_HOSTNAME_DEFAULT_VALUE, Importance.HIGH, YDB_HOSTNAME_DOC)
            .define(YDB_GRPC_PORT, Type.INT, YDB_GRPC_PORT_DEFAULT_VALUE, Importance.HIGH, YDB_GRPC_PORT_DOC)
            .define(YDB_GRPC_TLS_ENABLED, Type.BOOLEAN, YDB_GRPC_TLS_ENABLED_DEFAULT_VALUE, Importance.HIGH, YDB_GRPC_TLS_ENABLED_DOC)
            .define(YDB_DATABASE, Type.STRING, YDB_DATABASE_DEFAULT_VALUE, Importance.HIGH, YDB_DATABASE_DOC)
            .define(YDB_CONNECTION_STRING, Type.STRING, YDB_CONNECTION_STRING_DEFAULT_VALUE, Importance.MEDIUM, YDB_CONNECTION_STRING_DOC)
            .define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT_VALUE, Importance.MEDIUM, BATCH_SIZE_DOC)
            .define(YDB_AUTH_PROVIDER_TYPE, Type.STRING, YDB_AUTH_PROVIDER_TYPE_DEFAULT_VALUE, Importance.HIGH, YDB_AUTH_PROVIDER_TYPE_DOC)
            .define(YDB_AUTH_ACCESS_TOKEN, Type.STRING, YDB_AUTH_ACCESS_TOKEN_DEFAULT_VALUE, Importance.MEDIUM, YDB_AUTH_ACCESS_TOKEN_DOC)
            .define(YDB_AUTH_SA_KEY_FILE, Type.STRING, YDB_AUTH_SA_KEY_FILE_DEFAULT_VALUE, Importance.MEDIUM, YDB_AUTH_SA_KEY_FILE_DOC)
            .define(YDB_AUTH_USERNAME, Type.STRING, YDB_AUTH_USERNAME_DEFAULT_VALUE, Importance.MEDIUM, YDB_AUTH_USERNAME_DOC)
            .define(YDB_AUTH_PASSWORD, Type.STRING, YDB_AUTH_PASSWORD_DEFAULT_VALUE, Importance.MEDIUM, YDB_AUTH_PASSWORD_DOC)
            .define(MAX_RETRIES, Type.INT, MAX_RETRIES_DEFAULT_VALUE, Importance.LOW, MAX_RETRIES_DOC)
            .define(RETRY_BACKOFF_MS, Type.LONG, RETRY_BACKOFF_MS_DEFAULT_VALUE, Importance.LOW, RETRY_BACKOFF_MS_DOC)
            ;

    public static KafkaSinkConnectorConfig create(Map<String, String> props) {
        props.put(YDB_CONNECTION_STRING, makeConnectionString(props));

        return new KafkaSinkConnectorConfig(props);
    }

    private KafkaSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

    private static String makeConnectionString(Map<String, String> props) {
        String connectionString = props.get(YDB_CONNECTION_STRING);
        if (connectionString != null) {
            return connectionString;
        }

        String hostname = props.get(YDB_HOSTNAME);
        Integer port = Integer.parseInt(props.get(YDB_GRPC_PORT));
        String databaseName = props.get(YDB_DATABASE);
        String grpc = Boolean.parseBoolean(props.get(YDB_GRPC_TLS_ENABLED)) ? "grpcs" : "grpc";

        return String.format("%s://%s:%d%s", grpc, hostname, port, databaseName);
    }
}