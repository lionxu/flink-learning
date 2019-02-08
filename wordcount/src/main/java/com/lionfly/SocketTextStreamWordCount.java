package com.lionfly;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by lion_fly on 2019/01/28
 */
public class SocketTextStreamWordCount {
    /**
     *
     * @param args={"localhost", "9000"}
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length != 2){
            System.err.println("Usage:\nSocketTestStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        // set up the streaming execution environment
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据
        DataStreamSource<String> streamSource = environment.socketTextStream(hostname, port);

        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamSource.flatMap(new LineSpilt()).keyBy(0).sum(1);
        sum.print();
        environment.execute("Java WordCount from SocketTestStreamWordCount");
    }

    public static final class LineSpilt implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token: tokens) {
                if (token.length() > 0){
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
