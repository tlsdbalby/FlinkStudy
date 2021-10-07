package lby.study.apitest.transform;

import lby.study.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Sensor.txt");

        //转化成SensorReading类型 使用Lambda表达式简化书写
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0].trim(), Long.parseLong(strings[1].trim()), Double.parseDouble(strings[2].trim()));
        } );

        KeyedStream<SensorReading, Tuple> sensorReadingTupleKeyedStream = dataStream.keyBy("id");

        /*
        * reduce聚合, 得输出最大的温度值, 以及当前最新的时间戳
        * */
        SingleOutputStreamOperator<SensorReading> reduceStream = sensorReadingTupleKeyedStream.reduce((curState, newData) -> new SensorReading(curState.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature())));

        reduceStream.print();

        env.execute();
    }
}
