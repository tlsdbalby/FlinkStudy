package lby.study.apitest.source;

import lby.study.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env读取元素数据
        DataStream<SensorReading> dataStream1 = env.fromElements(
                new SensorReading("sensor_1", 1547718199l, 35.8),
                new SensorReading("sensor_6", 1547718201l, 15.4),
                new SensorReading("sensor_7", 1547718202l, 6.7),
                new SensorReading("sensor_10", 1547718205l, 38.1)
        );

        //env读取集合中的数据
        DataStream<SensorReading> dataStream2 = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199l, 35.8),
                        new SensorReading("sensor_6", 1547718201l, 15.4),
                        new SensorReading("sensor_7", 1547718202l, 6.7),
                        new SensorReading("sensor_10", 1547718205l, 38.1))
        );

        dataStream1.print("元素数据");
        dataStream2.print("集合中的数据");

        env.execute();

    }
}
