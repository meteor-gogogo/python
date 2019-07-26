package com.atguigu.loginDetect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, ValueState}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginDetect
  * Version: 1.0
  *
  * Created by wushengran on 2019/7/8 11:27
  */
// 输入的登录事件流
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )

// 输出的报警信息
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String )

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 自定义测试数据
    val loginStream = env.fromCollection( List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ) )
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)    // 根据用户id来做分组
      .process( new MatchFunction() )
      .print()

    env.execute("Login Fail Detect")
  }
}

class MatchFunction() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))

  //  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
  //    if(value.eventType == "fail"){
  //      loginState.add(value)
  //      ctx.timerService().registerEventTimeTimer( (value.eventTime + 2) * 1000 )
  //    }
  //    else
  //      loginState.clear()
  //  }

  //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
  //    val allLogins: ListBuffer[LoginEvent] = ListBuffer()
  //
  //    val iter = loginState.get().iterator()
  //    while( iter.hasNext )
  //      allLogins += iter.next()
  //
  //    loginState.clear()
  //
  //    // 如果state长度大于1，说明有两个以上的登录失败事件，输出报警信息
  //    if ( allLogins.length > 1 ){
  //      out.collect( Warning( allLogins.head.userId,
  //        allLogins.head.eventTime,
  //        allLogins.last.eventTime,
  //        "login fail in 2 seconds for " + allLogins.length + " times.") )
  //    }
  //  }
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 首先按照type做筛选，如果success直接清空，如果fail再做处理
    if ( value.eventType == "fail" ){
      // 如果已经有登录失败的数据，那么就判断是否在两秒内
      val iter = loginState.get().iterator()
      if ( iter.hasNext ){
        val firstFail = iter.next()
        // 如果两次登录失败时间间隔小于2秒，输出报警
        if ( value.eventTime < firstFail.eventTime + 2 ){
          out.collect( Warning( value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds." ) )
        }
        // 把最近一次的登录失败数据，更新写入state中
//        loginState.clear()
//        loginState.add(value)
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(value)
        loginState.update( failList )
      } else {
        // 如果state中没有登录失败的数据，那就直接添加进去
        loginState.add(value)
      }
    } else
      loginState.clear()
  }
}
