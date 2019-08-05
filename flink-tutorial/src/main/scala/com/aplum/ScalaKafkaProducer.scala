package com.aplum

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.codehaus.jackson.map.ser.StdSerializers.StringSerializer

import scala.io.Source

/*
 *@Author: liuhang
 *
 *@CreateTime: 2019-07-27 10:45
 *
 */

object ScalaKafkaProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.put("acks", "1")
    properties.put("retries", "3")
    properties.put("key.serializer", classOf[StringSerializer].getName)
    properties.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](properties)
    val lines: Iterator[String] = Source.fromFile("C:\\api.2019-07-24_10-00").getLines()
    while (lines.hasNext){
      val line: String = lines.next()
      val nObject: JSONObject = JSON.parseObject(line)
      val array: JSONArray = nObject.getJSONArray("ItemActions")
      while (array.iterator().hasNext){
        val unit: AnyRef = array.iterator().next()
        val jsonObject: JSONObject = JSON.parseObject(unit.toString)
        val lineNew =
          """
             |{"ClientIP":"%s","Createtime":"%s","id":"%s","position":"24","status":"onsale","Sid":"%s","Source":"%s","X-Pd-Identify":""}
          """.stripMargin.replaceAll("\r\n", "").format(nObject.getString("ClientIP"),
              nObject.getString("Createtime"),
              jsonObject.getString("id"),
            jsonObject.getString("position"),
            jsonObject.getString("status"),
            nObject.getString("Sid"),
            nObject.getString("Source"),
            nObject.getString("X-Pd-Identify")
            )
        val record = new ProducerRecord[String, String]("view", lineNew)
        producer.send(record, new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            if (recordMetadata != null){
              println("消息发送成功")
            }
            if(e != null){
              println("消息发送失败")
            }
          }
        })
      }
    }
    producer.close()
  }
}
