package com.aplum.hotitemsanalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Copyright (c) 2018-2028  All Rights Reserved
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitemsanalysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/7/8 9:08
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    val bufferedSource = io.Source.fromFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    val producer = new KafkaProducer[String, String](properties)

    for ( line <- bufferedSource.getLines() ){
      val record = new ProducerRecord[String, String]( "hotitems", line )
      println("one record is sending..")
      producer.send(record)
    }
    producer.close()
  }
}
