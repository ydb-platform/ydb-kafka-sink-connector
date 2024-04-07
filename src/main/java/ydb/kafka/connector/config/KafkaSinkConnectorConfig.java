package ydb.kafka.connector.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

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


    public static final String GRPC_TLS_PORT = "ydb.grpc.port";
    public static final Integer GRPC_TLS_PORT_DEFAULT_VALUE = 2136;
    public static final String GRPC_TLS_PORT_DOC = "Define YDB server grpc port";


    public static final String YDB_DATABASE = "ydb.database";
    public static final String YDB_DATABASE_DEFAULT_VALUE = "/local";
    public static final String YDB_DATABASE_DOC = "Define YDB sink database name";

    public static final String SINK_SERVER_CONNECTION_STRING = "ydb.connection.string";
    public static final String SINK_SERVER_CONNECTION_STRING_VALUE =
            makeConnectionString(
                    YDB_HOSTNAME_DEFAULT_VALUE,
                    GRPC_TLS_PORT_DEFAULT_VALUE,
                    YDB_DATABASE_DEFAULT_VALUE);
    public static final String SINK_SERVER_CONNECTION_DOC = "YDB connection string";

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT_VALUE = 1000;
    private static final String BATCH_SIZE_DOC =
            "Specifies how many records to attempt to batch together for insertion into the destination"
                    + " table, when possible.";

    public static ConfigDef CONFIG = new ConfigDef()
            .define(SOURCE_TOPIC, Type.STRING, SOURCE_TOPIC_DEFAULT_VALUE, Importance.HIGH, SOURCE_TOPIC_DOC)
            .define(SINK_BOOTSTRAP_SERVER, Type.STRING, SINK_BOOTSTRAP_SERVER_DEFAULT_VALUE, Importance.HIGH, SINK_BOOTSTRAP_SERVER_DOC)
            .define(YDB_HOSTNAME, Type.STRING, YDB_HOSTNAME_DEFAULT_VALUE, Importance.HIGH, YDB_HOSTNAME_DOC)
            .define(GRPC_TLS_PORT, Type.INT, GRPC_TLS_PORT_DEFAULT_VALUE, Importance.HIGH, GRPC_TLS_PORT_DOC)
            .define(YDB_DATABASE, Type.STRING, YDB_DATABASE_DEFAULT_VALUE, Importance.HIGH, YDB_DATABASE_DOC)
            .define(SINK_SERVER_CONNECTION_STRING, Type.STRING, SINK_SERVER_CONNECTION_STRING_VALUE, Importance.HIGH, SINK_SERVER_CONNECTION_DOC)
            .define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT_VALUE, Importance.MEDIUM, BATCH_SIZE_DOC)
            ;

    public static KafkaSinkConnectorConfig create(Map<String, String> props) {
        String hostname = props.get(YDB_HOSTNAME);
        Integer port = Integer.parseInt(props.get(GRPC_TLS_PORT));
        String databaseName = props.get(YDB_DATABASE);

        props.put(SINK_SERVER_CONNECTION_STRING, makeConnectionString(hostname, port, databaseName));

        return new KafkaSinkConnectorConfig(props);
    }

    private KafkaSinkConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

    private static String makeConnectionString(String hostname, Integer port, String databaseName) {
        return String.format("grpc://%s:%d%s", hostname, port, databaseName);
    }
}