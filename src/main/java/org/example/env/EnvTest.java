package org.example.env;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author guagua
 * @date 2023/1/8 21:14
 * @describe
 */
public class EnvTest {

    public static void main(String[] args) {

        // 创建一个本地的执行环境
        ExecutionEnvironment.createLocalEnvironment();
        // 创建本地执行环境，设置并行度
        ExecutionEnvironment.createLocalEnvironment(2);
        // flink 自动判断执行环境
        ExecutionEnvironment.getExecutionEnvironment();

        // 创建流执行环境
        StreamExecutionEnvironment.createLocalEnvironment();
        // 创建流执行环境, 并设置并行度
        StreamExecutionEnvironment.createLocalEnvironment(2);
        // 创建流执行环境, flink自动判断执行环境
        StreamExecutionEnvironment.getExecutionEnvironment();

    }
}
