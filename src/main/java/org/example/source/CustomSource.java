package org.example.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.pojo.SensorReading;

import java.util.HashMap;
import java.util.Random;

/**
 * @author guagua
 * @date 2023/1/11 17:15
 * @describe
 */
public class CustomSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySourceFunction mySourceFunction = new MySourceFunction();
        DataStreamSource<SensorReading> dataSource = env.addSource(mySourceFunction);

        dataSource.print();



        env.execute();
    }

}

class MySourceFunction implements SourceFunction<SensorReading>{

    private boolean isRunning = true;
    private Random random = new Random();


    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        HashMap<String, Double> sensor = new HashMap<>();
        for (int i = 1; i <=10 ; i++) {
            sensor.put("sensor_" + i, 60 + random.nextGaussian() * 20);
        }
        while (isRunning) {
            for (String sensorId : sensor.keySet()) {
                double newTemp = sensor.get(sensorId) + random.nextGaussian();
                sensor.put(sensorId, newTemp);
                SensorReading sensorReading = new SensorReading(sensorId, System.currentTimeMillis(), newTemp);
                ctx.collect(sensorReading);
            }
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}

