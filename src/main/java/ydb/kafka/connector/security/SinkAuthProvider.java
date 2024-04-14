package ydb.kafka.connector.security;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import tech.ydb.auth.AuthRpcProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.core.impl.auth.GrpcAuthRpc;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.StaticCredentials;

import static ydb.kafka.connector.config.KafkaSinkConnectorConfig.*;

@Slf4j
public class SinkAuthProvider {
    private final AuthRpcProvider<? super GrpcAuthRpc> authProvider;

    public SinkAuthProvider(KafkaSinkConnectorConfig config) {
        AuthProviderType authProviderType = AuthProviderType.valueOf(config.getString(YDB_AUTH_PROVIDER_TYPE));
        switch (authProviderType) {
            case ANONYMOUS:
                this.authProvider = NopAuthProvider.INSTANCE;
                break;
            case ACCESS_TOKEN:
                String accessToken = config.getString(YDB_AUTH_ACCESS_TOKEN);
                this.authProvider = new TokenAuthProvider(accessToken);
                break;
            case METADATA:
                this.authProvider = CloudAuthHelper.getMetadataAuthProvider();
                break;
            case SERVICE_ACCOUNT_KEY:
                String saKeyFile = config.getString(YDB_AUTH_SA_KEY_FILE);
                this.authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);
                break;
            case ENVIRON:
                this.authProvider = CloudAuthHelper.getAuthProviderFromEnviron();
                break;
            case STATIC_CREDENTIALS:
                String username = config.getString(YDB_AUTH_USERNAME);
                String password = config.getString(YDB_AUTH_PASSWORD);
                this.authProvider = new StaticCredentials(username, password);
                break;
            default:
                throw new InvalidConfigurationException("Invalid auth provider type");
        }
        log.info("{} auth provider has been created.", authProviderType);
    }

    public AuthRpcProvider<? super GrpcAuthRpc> get() {
        return authProvider;
    }

}
