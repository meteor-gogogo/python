package com.aplum

import java.util
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.JavaConversions._
import scala.collection.mutable
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommandDescription, RedisMapper}

/*
 *@Author: liuhang
 *
 *@CreateTime: 2019-07-25 16:24
 *
 */
//{"ClientIP":"223.157.237.79","Createtime":1563933604,"CurrentURL":"https://app.aplum.com/product/personal-page?cid=10\u0026user_identity=e33a67557c5c2710f3607736bd696df245cee80a2b947364b909e5cb97b6f71648997849","ItemActions":[{"id":2853392,"position":0,"status":"onsale"},{"id":2848793,"position":1,"status":"onsale"},{"id":2847421,"position":2,"status":"onsale"},{"id":2847353,"position":3,"status":"onsale"}],"Sid":"444405308ad0c610b50a23fdedf938d6","Source":"plum","UserAgent":"Mozilla/5.0 (iPhone; CPU iPhone OS 11_2_5 like Mac OS X) AppleWebKit/604.5.6 (KHTML, like Gecko) Mobile/15D60 iosapp version:(2.5.5)","Vfm":"other:personal","X-Pd-Carrier":"","X-Pd-Identify":"48997849","X-Pd-Idfa":"F87ACD40-8CCB-46ED-B466-7231B36F4AE9","X-Pd-Imei":""}
//输入数据样例类
case class ProductView(clientIp: String, createTime: Long, pid: Int, position: Int, status: String, sid: String, source: String, uid: Int)

object StreamViewDataToRedis {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val hashMap = new mutable.HashMap[Int, String]()
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100)
    //每五分钟获取一遍product数据
    val productStream: DataStream[mutable.HashMap[Int, String]] = env.addSource(JDBCReader)
    productStream.broadcast
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("view", new SimpleStringSchema(), properties))
    stream.print()

    val value: DataStream[List[(String, Int)]] = stream.map(line => {
      val json: JSONObject = JSON.parseObject(line)
      val jSONArray: JSONArray = json.getJSONArray("ItemActions")
      val list: List[AnyRef] = jSONArray.iterator().toList
      val views: List[ProductView] = list.map(m => {
        val subJsonObject: JSONObject = JSON.parseObject(m.toString)
        ProductView(json.getString("ClientIP"), json.getLong("Createtime"), subJsonObject.getInteger("id"), subJsonObject.getInteger("position"), subJsonObject.getString("status"), json.getString("Sid"), json.getString("Source"), json.getInteger("X-Pd-Identify"))
      })
      views
    })
      .map(listTmp => {
        listTmp.map(product => {
          (product.clientIp.toString, 1)
          (product.createTime.toString, 1)
          (product.pid.toString, 1)
          (product.position.toString, 1)
          (product.sid.toString, 1)
          (product.source.toString, 1)
          (product.status.toString, 1)
          (product.uid.toString, 1)
        })
      })
    value.print("fields>>>>>>>>>>>>>")

    env.execute("consume kafka")
  }

}
//class MyRedis extends RedisMapper[ProductView]{
//  override def getCommandDescription: RedisCommandDescription = ???
//
//  override def getValueFromData(t: ProductView): String = ???
//
//  override def getKeyFromData(t: ProductView): String = ???
//}

