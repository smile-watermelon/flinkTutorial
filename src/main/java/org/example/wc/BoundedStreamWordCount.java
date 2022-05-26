package org.example.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import sun.security.provider.Sun;

/**
 * 流处理
 */
public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
//        1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        2、读取文件
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");
//        3、切割单词，获取元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = dataStreamSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
//        4、根据key进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = operator.keyBy(item -> item.f0);
//        5、分组求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
//        6、打印
        sum.print();

//        7、执行
        env.execute();

    }


}
