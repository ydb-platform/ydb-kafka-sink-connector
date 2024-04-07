package ydb.kafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import ydb.kafka.connector.config.KafkaSinkConnectorConfig;
import ydb.kafka.connector.db.YdbWriter;

import java.util.*;

@Slf4j
public class KafkaSinkTask extends SinkTask {
    private KafkaSinkConnectorConfig config;
    private String connectorName;
    private YdbWriter ydbWriter;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            log.info("Task config setting : " + props.toString());
            this.connectorName = props.get("name");
            this.config = KafkaSinkConnectorConfig.create(props);
            this.ydbWriter = new YdbWriter(config);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            ydbWriter.write(records);
        } catch (Exception e) {
            log.error(e.getMessage() + " / " + connectorName, e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void stop() {
        ydbWriter.close();
    }
}