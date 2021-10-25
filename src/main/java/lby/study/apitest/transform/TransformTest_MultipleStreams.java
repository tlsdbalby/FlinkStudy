package lby.study.apitest.transform;

import lby.study.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class TransformTest_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Sensor.txt");

        //转化成SensorReading类型 使用Lambda表达式简化书写
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] strings = line.split(",");
            return new SensorReading(strings[0].trim(), Long.parseLong(strings[1].trim()), Double.parseDouble(strings[2].trim()));
        } );

        //1.split+select操作实现分流: split为数据打标记 select提取指定标记的数据以构成新流, 从而实现分流操作. 采用这样的分流可以加大分流的灵活性, 根据标记组合可以很灵活地获取子流
        //按照temperature的取值将原流分为两个流, 高温和低温流, 然后获得高温流
        //但这个方法已经被flink 1.12弃用了....
        SplitStream<SensorReading> splitStream = dataStream.split((sensorReading) -> (sensorReading.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low")));

        DataStream<SensorReading> highTemperatureStream = splitStream.select("high");
        DataStream<SensorReading> lowTemperatureStream = splitStream.select("low");
        DataStream<SensorReading> allTemperatureStream = splitStream.select("high", "low");

        highTemperatureStream.print("highStream");
        lowTemperatureStream.print("lowStream");
        allTemperatureStream.print("allStream");

        //将高温流转换成二元组类型, 然后将其与低温流进行合并
        DataStream<Tuple2<String, Double>> warningStream = highTemperatureStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        //2.两条流合流connect:将高温元组流和低温流合并, 此时只是形式合流, 此流中具有两种数据形式
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warningStream.connect(lowTemperatureStream);
        //合流-统一输出:此时要将两种数据形式进行统一, 是真正意义上的合流, 返回得到一个DataStream
        SingleOutputStreamOperator<Object> combineStream = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<String, Double, String>(value.f0, value.f1, "高温！");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple3<String, Double, String>(value.getId(), value.getTemperature(), "低温！");
            }
        });
        combineStream.print("combine");

        //3.多流合并union:支持多条流的合并, 但多条流的数据类型必须保持一致! 用connect进行多流合并要两两合并再和其他合并, 所以n条流需要合并n-1次, 非常繁琐!
        highTemperatureStream.union(lowTemperatureStream).union(lowTemperatureStream);

        env.execute();
    }
}
