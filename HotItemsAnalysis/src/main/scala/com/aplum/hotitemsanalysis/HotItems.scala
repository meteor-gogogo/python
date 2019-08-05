package com.aplum.hotitemsanalysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028  All Rights Reserved
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.HotItemsAnalysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/7/6 11:40
  */
// 输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )

// 中间聚合结果样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )

object HotItems {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 指定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100)

    // 读取数据流进行处理
//    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      val dataStream = env.addSource( new FlinkKafkaConsumer[String]( "hotitems", new SimpleStringSchema(), properties ) )
      .map( line => {
        val lineArray = line.split(",")
        UserBehavior( lineArray(0).trim.toLong, lineArray(1).trim.toLong, lineArray(2).trim.toInt, lineArray(3).trim, lineArray(4).trim.toLong  )
      } )
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
//        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000
//      })
      .assignAscendingTimestamps( _.timestamp * 1000 )
      .filter(_.behavior == "pv")   // 过滤出点击浏览事件
      .keyBy(_.itemId)    // 按照itemId分区
//        .window( SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(5)) )
      .timeWindow( Time.minutes(60), Time.minutes(5) )    // 开时间窗口，滑动窗口
      .aggregate( new CountAgg(), new WindowResultFunction() )
      .keyBy(_.windowEnd)
      .process( new TopNHotItems(3) )
      .print("items")

    // 调用execute()执行任务
    env.execute("Hot Items")
  }
}

// 预聚合操作，来一条数据就计数器加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 窗口关闭时的操作，包装成ItemViewCount
class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

class TopNHotItems(size: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取当前运行环境中的 ListState，用来回复itemState
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]( "item-state", classOf[ItemViewCount] )
    itemState = getRuntimeContext.getListState(itemStateDesc)
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据都暂存入liststate
    itemState.add(value)
    // 注册一个定时器，延迟触发
    ctx.timerService().registerEventTimeTimer( value.windowEnd + 100 )
  }

  // 核心处理流程，定时器触发时进行操作，可以认为之前的数据都已经到达
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    for( item <- itemState.get() ){
      allItems += item
    }
    // 提前清除状态数据
    itemState.clear()

    // 按照count大小排序选择前size个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(size)

    // 将最终结果转换为String输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 100)).append("\n")

    for( i <- 0 until sortedItems.length ){
      val currentItem = sortedItems(i)
      // 输出格式： NO, ID, count
      result.append("NO").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }

    result.append("====================================\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
