package com.atguigu.networktraffic

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networktraffic
  * Version: 1.0
  *
  * Created by wushengran on 2019/7/8 10:20
  */

// 输入web log数据流
case class ApacheLogEvent( ip: String, userName: String, eventTime: Long, method: String, url: String )

// 中间统计数量的数据类型
case class UrlViewCount( url: String, windowEnd: Long, count: Long )

object NetworkTraffic {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apache.log")
      .map( data => {
        val dataArray = data.split(" ")
        // 把log时间转换为时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent( dataArray(0), dataArray(2), timestamp, dataArray(5), dataArray(6) )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          // 已经是毫秒
          element.eventTime
        }
      } )
      .keyBy(_.url)  // 根据url分组
      .timeWindow( Time.minutes(1), Time.seconds(5) )    // 开滑动窗口
      .aggregate( new CountAgg(), new WindowResultFuntion() )
      .keyBy(_.windowEnd)    // 根据时间窗口来分组
      .process( new TopNUrls(5) )
      .print()    // sink 输出

    env.execute("Network Traffic Analysis")
  }
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResultFuntion() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(url: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount( url, window.getEnd, input.iterator.next() ))
  }
}

class TopNUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
  // 懒加载方式定义state
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState( new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    // 注册定时器，当定时器触发时，应该收集到了所有的数据
    ctx.timerService().registerEventTimeTimer( value.windowEnd + 100 )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从state中获取所有的数据
    val allUrlViewCounts: ListBuffer[UrlViewCount] = ListBuffer()

//    import scala.collection.JavaConversions._
//    for( urlViewCount <- urlState.get() ){
//      allUrlViewCounts += urlViewCount
//    }
    val iter = urlState.get().iterator()
    while( iter.hasNext )
      allUrlViewCounts += iter.next()

    urlState.clear()

    // 按照点击量大小排序
    val sortedUrlViewCounts = allUrlViewCounts.sortWith(_.count>_.count).take(topSize)

    // 把结果格式化为string输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n")

    for (i <- sortedUrlViewCounts.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViewCounts(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")

    Thread.sleep(500)

    out.collect(result.toString())
  }
}


