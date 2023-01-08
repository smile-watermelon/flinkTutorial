package org.example.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author guagua
 * @date 2023/1/8 21:24
 * @describe
 */
public class FileSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/guagua/code/my/flink/input/sensor.txt");

        dataStreamSource.print();

        env.execute();
    }
}
