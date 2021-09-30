package lby.study.apitest.source;

import lby.study.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据, 注意此时读到的数据是String, 要经过手动包装才能转化为SensorReading
        DataStream<String> dataStream = env.readTextFile("D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Sensor.txt").setParallelism(2);
        DataStream<SensorReading> sensorReadingDataStream = dataStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] strings = s.split(",");
                return new SensorReading(strings[0].trim(), Long.parseLong(strings[1].trim()), Double.parseDouble(strings[2].trim()));
            }
        }).setParallelism(2);
        sensorReadingDataStream.print().setParallelism(1);
        env.execute();

    }
}
