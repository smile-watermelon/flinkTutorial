package org.example.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * 流处理，单词统计
 *
 * 并行度的优先级
 * 每个算子 > env全局 > 提交job > 集群配置默认值
 * ./bin/flink run -c org.example.wc.StreamWordCount -p 3 /Users/guagua/code/my/flink/target/flink-1.0-SNAPSHOT.jar --host localhost --port 7777
 * 提交的任务可以运行，至少需要 并行度 1/2 的 slot
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //        获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port");
//        2、读取文本流
        DataStreamSource<String> source = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> op = source.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        })
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = op.keyBy(item -> item.f0);

        // 每一个操作都可以设置并行度
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1).setParallelism(1);

        sum.print();
//        sum.print().setParallelism(1);

        env.execute();
    }
}
