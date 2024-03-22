PROJECT_DIR=`pwd`
KAFKA_DIR='C:/tmp'

echo "BUILDING"
./gradlew prepareKafkaConnectDependencies
mv build/libs/* $KAFKA_DIR/libs
cp -r properties $KAFKA_DIR
cd $KAFKA_DIR
echo "START"
./bin/windows/connect-standalone.bat properties/worker.properties properties/ydb-sink.properties
cd $PROJECT_DIR