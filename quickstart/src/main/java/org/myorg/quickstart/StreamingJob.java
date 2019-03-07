/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import java.sql.Timestamp;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.model.UserBehavior;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//UserBehavior.csv的本地文件的路径
		URL fileUrl = StreamingJob.class.getClassLoader().getResource("UserBehavior.csv");
		Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
		//抽取UserBehavior的TypeInformation,是一个PojoTypeInfo
		PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
		//由于java反射提取出的字段顺序是不确定的,需要显示指定下文件中字段的顺序.
		String[] fileOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
		//创建PojoCsvInputFormat
		PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fileOrder);
		DataStream<UserBehavior> dataStream = env.createInput(csvInputFormat, pojoTypeInfo);
		DataStream<UserBehavior> timedData = dataStream
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
					@Override
					public long extractAscendingTimestamp(UserBehavior userBehavior) {
						//原始数据单位是秒,将其转化成毫秒
						return userBehavior.timestamp * 1000;
					}
				});
		DataStream<UserBehavior> pvData = timedData
				.filter(new FilterFunction<UserBehavior>() {
					@Override
					public boolean filter(UserBehavior userBehavior) throws Exception {
						//过滤出只有点击的数据
						return userBehavior.behavior.equals("pv");
					}
				});
		DataStream<ItemViewCount> windowedData = pvData
				.keyBy("itemId")
				.timeWindow(Time.hours(1), Time.minutes(5))
				.aggregate(new CountAgg(), new WindowResultFunction());
		DataStream<String> topItems = windowedData
				.keyBy("windowEnd")
				.process(new TopNHotItems(3));
		topItems.print();
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	/**
	 * 商品点击量(窗口操作的输出类型)
	 */
	public static class ItemViewCount{
		public long itemId;		// 商品ID
		public long windowEnd;	// 窗口结束时间戳
		public long viewCount;	// 商品的点击量

		public static ItemViewCount of(long itemId, long windowEnd, long viewCount){
			ItemViewCount result = new ItemViewCount();
			result.itemId = itemId;
			result.windowEnd = windowEnd;
			result.viewCount = viewCount;
			return result;
		}
	}

	/**
	 * 用于输出窗口的结果
	 */
	public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{
		@Override
		public void apply(
				Tuple tuple,
				TimeWindow timeWindow,
				Iterable<Long> iterable,
				Collector<ItemViewCount> collector
		) throws Exception {
			Long itemId = ((Tuple1<Long>)tuple).f0;
			Long count = iterable.iterator().next();
			collector.collect(ItemViewCount.of(itemId, timeWindow.getEnd(), count));
		}
	}

	public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long>{
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(UserBehavior userBehavior, Long aLong) {
			return aLong + 1;
		}

		@Override
		public Long getResult(Long aLong) {
			return aLong;
		}

		@Override
		public Long merge(Long aLong, Long acc1) {
			return aLong + acc1;
		}
	}

	public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{

		private final int topSize;

		public TopNHotItems(int topSize){
			this.topSize = topSize;
		}

		//用于存储商品与点击数的状态,待收齐同一个窗口的数据后,再触发TopN计算
		private ListState<ItemViewCount> itemState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			//状态的注册
			ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<ItemViewCount>(
					"itemState-state",
					ItemViewCount.class
			);
			itemState = getRuntimeContext().getListState(itemStateDesc);
		}

		@Override
		public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
			//每条数据都保存到状态中
			itemState.add(itemViewCount);
			// 注册windowEnd+1的EventTime Timer,当触发时,说明收齐了属于windowEnd窗口的所有商品数据
			context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 1);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			super.onTimer(timestamp, ctx, out);
			//获取收到的所有商品点击量
			List<ItemViewCount> allItems = new ArrayList<>();
			for (ItemViewCount item: itemState.get()) {
				allItems.add(item);
			}
			//提前清除状态中的数据释放空间
			itemState.clear();
			allItems.sort(new Comparator<ItemViewCount>() {
				@Override
				public int compare(ItemViewCount o1, ItemViewCount o2) {
					return (int)(o2.viewCount - o1.viewCount);
				}
			});

			// 将排名信息格式化成string,便于打印
			StringBuilder result = new StringBuilder();
			result.append("=================================\n");
			result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n");
			for (int i = 0; i < topSize; i++) {
				ItemViewCount currentItem = allItems.get(i);
				result.append("No").append(i).append(":")
						.append(" 商品ID=").append(currentItem.itemId)
						.append(" 浏览量=").append(currentItem.viewCount)
						.append("\n");
			}
			result.append("=================================\n");
			out.collect(result.toString());
		}
	}
}
