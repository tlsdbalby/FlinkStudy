package lby.study.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 大数据计算核心 之 转换算子Transform
*   基本算子: Map, FlatMap, Filter
* */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Sensor.txt").setParallelism(2);

        //将每行输入按照逗号切成一个个的字符串, 并转化成<字符串, 长度>形式, 输入输出数量关系 1:n, 因此采用FlatMap
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(",");
                for (String str : split) {
                    collector.collect(new Tuple2<String, Integer>(str.trim(), str.trim().length()));
                }
            }
        });

        //将输入的<字符串, 长度> 转成 <纯大写字符串, 长度*2>, 输入输出 1:1, 因此采用Map
        DataStream<Tuple2<String, Integer>> mapStream = flatMapStream.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple) throws Exception {
                tuple.setFields(tuple.f0.toUpperCase(), tuple.f1 * 2);
                return tuple;
            }
        });

        //过滤长度少于8或者大于20的输入
        DataStream<Tuple2<String, Integer>> filterStream = mapStream.filter(new FilterFunction<Tuple2<String, Integer>>() {
            public boolean filter(Tuple2<String, Integer> tuple) throws Exception {
                return  !(tuple.f1 < 10 || tuple.f1 > 16);
            }
        });

        filterStream.print();

        env.execute();
    }
}
