package lby.study.apitest.source;

import lby.study.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/*
* 读取自定义的数据源(内部数据源), 由用户设定数据的生成, 由Flink实现流的模拟
* */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.addSource( new mySensorSource());

        dataStream.print();

        env.execute();
    }

    public static class mySensorSource implements SourceFunction<SensorReading>{
        //定义一个标志位, 控制数据的产生
        private boolean running = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random rd = new Random();
            HashMap<String, Double> hashMap = new HashMap<String, Double>();
            for (int i = 0; i < 10; i++) {
                hashMap.put("sensor_"+(i+1), 60 + rd.nextGaussian() * 20);
            }
            while (running){
                for (String sensorId : hashMap.keySet()) {
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), hashMap.get(sensorId)));
                    hashMap.put(sensorId, hashMap.get(sensorId) + rd.nextGaussian());
                }
                Thread.sleep(1000);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
