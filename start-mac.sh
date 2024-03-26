PROJECT_DIR=`pwd`
KAFKA_DIR='docker-compose/tmp/kafka'

#echo "DOWNLOADING"
#mkdir $KAFKA_DIR
#cd $KAFKA_DIR
#cd ..
#wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
#tar -xvf kafka_2.13-3.6.1.tgz --strip 1 --directory ./kafka/

echo "BUILDING"
./gradlew prepareKafkaConnectDependencies
mv build/libs/* $KAFKA_DIR/libs
cp -r properties $KAFKA_DIR
cd $KAFKA_DIR
echo "START"
./bin/connect-standalone.sh properties/worker.properties properties/ydb-sink.properties
cd $PROJECT_DIR