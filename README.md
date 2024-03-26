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

- Kafka Security
- YDB Security
- Batch
  - JDBC +
  - YB +
  - YDB -
- Insert mode
  - insert, upsert, update
- Retry (maximum, backoff)
  - JDBC +
  - YB +
  - YDB -
- Kafka Headers
  - YB connector умеет сохранять, стандартный JDBC не умеет (как надо нам?)
- Table Primary Key
  - JDBC none, kafka (topic,partition,offset), record_key, record_value
  - YB: same jdbc https://github.com/yugabyte/yb-kafka-sink/blob/798e28d8a1882825bdb7ac4f3d116d25b6e8f1f3/sink/src/main/java/com/yugabyte/jdbc/sink/PreparedStatementBinder.java#L89-L132
  - YDB kafka (partition,offset)
- А оно нам надо?
  - Timestamp
  - Error tolerance
  - Dead letter queue topic name
  - Несколько топиков из одного коннектора
- Нужна валидация исключений на ошибки при подключении и тд: https://docs.redpanda.com/current/deploy/deployment-option/cloud/managed-connectors/create-jdbc-sink-connector/#troubleshoot
- Реализация записи в таблицы (нужен доресерч):
  - JDBC Sink имеет возможность записи json с заданной схемой (записи делаются в отдельную таблицу, где каждое поле это столбец в таблице)
  - YB Sink

Сравнение с:
 - YB Sink
 - Cockroach Sink / maybe https://github.com/fabiog1901/kafka2cockroachdb
 - JDBC Sink
 - Cockroach CDC https://www.cockroachlabs.com/docs/stable/change-data-capture-overview
Нужно определить критерии сравнения