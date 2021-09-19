package lby.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 流处理WordCount
* */
public class StreamWordCount {
    public static void main(String[] args) {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //从文件中读取数据, 但这种数据大部分是有界的, 不是流数据所擅长的无界数据
//        String inputPath = "D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Hello.txt";
//        DataStream<String> inputStream = env.readTextFile(inputPath);

        //生产环境下使用传参的方式确定参数, 此处利用Java程序的入口传参
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

//        host = "192.168.51.129";
//        port = 7777;
        //启动一个Socket数据源, 读取流式数据, 此处是虚拟机的地址和端口
        DataStream<String> inputStream = env.socketTextStream(host, port);

        //基于数据流进行转换计算
        inputStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strs = s.split(" ");
                for (String str : strs) {
                    collector.collect(new Tuple2<String, Integer>(str, 1));
                }
            }
        }).keyBy(0).sum(1).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
