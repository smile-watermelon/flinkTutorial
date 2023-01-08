package org.example.source;

import com.sun.xml.internal.ws.api.message.MessageWritable;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.SensorReading;

import java.util.Arrays;

/**
 * @author guagua
 * @date 2023/1/8 21:18
 * @describe
 */
public class CollectionSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度，有序输出
        env.setParallelism(1);

        DataStreamSource<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor1", 1L, 28.5),
                new SensorReading("sensor2", 2L, 31.0),
                new SensorReading("sensor3", 3L, 27.2)
        ));

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 3, 5, 2, 6);

        dataStreamSource.print("data");

        integerDataStreamSource.print("int");

        env.execute();
    }
}
