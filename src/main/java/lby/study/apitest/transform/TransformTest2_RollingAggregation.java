package lby.study.apitest.transform;

import lby.study.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Sensor.txt");

        //转化成SensorReading类型 使用Lambda表达式简化书写
        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0].trim(), Long.parseLong(strings[1].trim()), Double.parseDouble(strings[2].trim()));
        } );

        /*
        * keyby() 的参数:
        *   1.若当前流是Tuple类型的数据, 则可以使用int position作为参数传入, position是key在Tuple数据中的位置
        *   2.若当前流是pojo类型, 则可以使用key在pojo中的变量名即String keyName作为参数传入, 此处采用本方法
        *   3.若当前流是pojo类型, 也可以使用实现keySelector接口的方式, 采用Lambda表达式 或 双冒号来简化书写
        * */
        KeyedStream<SensorReading, Tuple> sensorReadingTupleKeyedStream = dataStream.keyBy("id");

//        KeyedStream<SensorReading, Tuple> sensorReadingTupleKeyedStream = dataStream.keyBy( SensorReading::getId );

        SingleOutputStreamOperator<SensorReading> temperature = sensorReadingTupleKeyedStream.maxBy("temperature");

        temperature.print();


        env.execute();
    }
}
