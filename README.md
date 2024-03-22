# ydb-kafka-sink-connector

https://github.com/ydb-platform/ydb/wiki/Student-Projects#implementation-ydb-kafka-connection-sink

## Старт

```bash
docker compose up -d
./gradlew prepareKafkaConnectDependencies
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xvf kafka_2.13-3.6.1.tgz --strip 1 --directory ./kafka/
mv build/libs kafka/libs
./kafka/bin/connect-standalone properties/worker.properties properties/ydb-sink.properties
```

### Открытые вопросы
