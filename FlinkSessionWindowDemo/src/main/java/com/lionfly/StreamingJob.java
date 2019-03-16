package com.lionfly;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        long delay = 5000L;
        long windowGap = 10000L;

        DataStream<Tuple3<String, String, Long>> source = env.addSource(new DataStreamSource());
        DataStream<Tuple3<String, String, Long>> stream = source.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        System.out.println(element.f0 + "\t"
                                + element.f1
                                + "\twatermark-> " + format.format(getCurrentWatermark().getTimestamp())
                                + " timestamp-> " + format.format(element.f2)
                        );
                        return element.f2;
                    }
                }
        );
        stream
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(windowGap)))
                .apply(new WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, String, Long>> iterable, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        System.out.println("start = " + format.format(timeWindow.getStart()) + ", end = " + format.format(timeWindow.getEnd()));
                        Tuple3<String, String, Long> result = Tuple3.of("", "", 1L);
                        for (Tuple3<String, String, Long> value: iterable) {
                            result = Tuple3.of(value.f0, result.f1 + value.f1, 1L);
                        }
                        collector.collect(result);
                    }
                })
                .print();
        env.execute("Flink Streaming Java API Skeleton");
    }
}
