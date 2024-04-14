package ydb.kafka.connector.security;

public enum AuthProviderType {
    ANONYMOUS,
    ACCESS_TOKEN,
    METADATA,
    SERVICE_ACCOUNT_KEY,
    ENVIRON,
    STATIC_CREDENTIALS
}

