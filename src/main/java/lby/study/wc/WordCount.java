package lby.study.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/*
* 批处理WordCount
* */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境, 实例化flink执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据, 构造数据源
        String inputPath = "D:\\JetBrains\\IntelliJ WorkSpace\\FlinkStudy\\src\\main\\resources\\Hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //对数据源进行处理, 按空格分词 转换成 <单词, 频率(1)> 的键值对
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strs = s.split(" ");
                for (String str : strs) {
                    collector.collect(new Tuple2<String, Integer>(str, 1));
                }
            }
        }).groupBy(0).sum(1);
        resultSet.print();

    }
}
