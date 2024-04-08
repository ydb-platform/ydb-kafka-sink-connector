# ydb-kafka-sink-connector

https://github.com/ydb-platform/ydb/wiki/Student-Projects#implementation-ydb-kafka-connection-sink

## Старт

```bash
PROJECT_DIR=$(pwd)
COMPOSE_DIR='docker-compose'
KAFKA_DIR="$COMPOSE_DIR/tmp/kafka"

echo "DOWNLOADING"
mkdir -p $KAFKA_DIR && cd $COMPOSE_DIR/tmp || exit
wget https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz
tar -xvf kafka_2.13-3.6.2.tgz --strip 1 --directory ./kafka/

echo "BUILD AND COPY"
cd "$PROJECT_DIR" || exit
./gradlew prepareKafkaConnectDependencies
mv build/libs/* $KAFKA_DIR/libs
cp -r properties $KAFKA_DIR

echo "START"
cd "$PROJECT_DIR" || exit
mkdir $COMPOSE_DIR/ydb_certs $COMPOSE_DIR/ydb_data
docker compose -f ./docker-compose/docker-compose.yml up -d --wait --quiet-pull
cd $KAFKA_DIR || exit
./bin/connect-standalone.sh properties/worker.properties properties/ydb-sink.properties
cd "$PROJECT_DIR" || exit
```
или
```bash
./start.sh
```

После запуска можно проверить работу коннектора, отправив сообщение в топик `mytopic`.

В YDB должна появиться запись с ключом `key` и значением `value` в таблице соответствующей названию топика.

Например:
```bash
docker-compose/tmp/kafka/bin/kafka-console-producer.sh  --broker-list localhost:9092 --topic mytopic --property parse.key=true --property key.separator=,
>key,value
> // CTRL + C
```
В таблице `mytopic` в YDB увидим следующее:

| key | offset | partition | value   |
|-----|--------|-----------|---------|
| key | 0      | 0         | value   |
