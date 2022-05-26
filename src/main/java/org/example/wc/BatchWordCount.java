package org.example.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
//        1、创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//        2、读取数据
        DataSource<String> dataSource = environment.readTextFile("input/words.txt");
//        3、将每行数据进行切割，转换成二元组
        FlatMapOperator<String, Tuple2<String, Integer>> tuple = dataSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4、根据元组第一个key进行分组
        UnsortedGrouping<Tuple2<String, Integer>> group = tuple.groupBy(0);
//        5、统计
        AggregateOperator<Tuple2<String, Integer>> sum = group.sum(1);
//        6、答应输出
        sum.print();

    }
}
