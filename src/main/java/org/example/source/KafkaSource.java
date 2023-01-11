package org.example.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.constant.KafkaConstant;

import java.util.Properties;

/**
 * @author guagua
 * @date 2023/1/11 16:29
 * @describe
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(KafkaConstant.BOOTSTRAP_SERVERS_NAME, KafkaConstant.BOOTSTRAP_SERVERS);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> dataStreamSource = environment.addSource(kafkaConsumer);

        dataStreamSource.print();

        environment.execute();
    }
}
