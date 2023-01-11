package org.example.transform;

import com.sun.xml.internal.ws.api.message.MessageWritable;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.SensorReading;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author guagua
 * @date 2023/1/11 17:52
 * @describe
 */
public class BaseTransform {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/guagua/code/my/flink/input/sensor.txt");
        SingleOutputStreamOperator<Integer> map = dataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        SingleOutputStreamOperator<SensorReading> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String s, Collector<SensorReading> collector) throws Exception {
                String[] split = s.split(",");
                List<String> collect = Arrays.stream(split).map(String::trim).collect(Collectors.toList());
                SensorReading sensorReading = new SensorReading(collect.get(0), Long.parseLong(collect.get(1)), Double.parseDouble(collect.get(2)));
                collector.collect(sensorReading);

            }
        });

        SingleOutputStreamOperator<String> flatMap1 = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }

            }
        });

        SingleOutputStreamOperator<String> filter = dataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor1");
            }
        });
//        map.print();
//        flatMap.print();
//        flatMap1.print();
//        filter.print();
//        dataStreamSource.print();

        env.execute();

    }
}
