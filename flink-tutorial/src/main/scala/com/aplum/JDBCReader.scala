package com.aplum

import java.sql._
import java.util.Properties

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object JDBCReader extends RichSourceFunction[mutable.HashMap[Int, String]] {
  private val logger: Logger = LoggerFactory.getLogger(JDBCReader.getClass)

  private var ps: PreparedStatement = null
  private var isRunning: Boolean = true
  private var connection: Connection = null

  def open() = {
    val driver = "com.mysql.jdbc.Driver"
    val readConfig = new ReadConfig()
    val properties: Properties = readConfig.loadProperties()
    Class.forName(driver)
    val url = "jdbc:mysql://" + properties.getProperty("host") + ":" + properties.getProperty("port") + "/" + properties.getProperty("database") + "?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    connection = DriverManager.getConnection(url, properties.getProperty("user"), properties.getProperty("password"))
    ps = connection.prepareStatement("select id, bid, cid, original_price, discount_price, sale_price, discount_rate from t_product")
  }

  override def cancel(): Unit = {
    try {
      super.close()
      if (connection != null) {
        connection.close()
      }
      if (ps != null) {
        ps.close()
      }
    } catch {
      case e: Exception => {
        logger.error("cancel Exception:{}", e)
      }
    }
    isRunning = false

  }

  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[Int, String]]): Unit = {
    val hashMap = new mutable.HashMap[Int, String]()
    try {
      while (isRunning) {
        val resultSet: ResultSet = ps.executeQuery()
        while (resultSet.next()) {
          var pid: Int = resultSet.getInt("id")
          var bid: Int = resultSet.getInt("bid")
          var cid: Int = resultSet.getInt("cid")
          var original_price: Float = resultSet.getFloat("original_price")
          var discount_price: Float = resultSet.getFloat("discount_price")
          var sale_price: Float = resultSet.getFloat("sale_price")
          var discount_rate: Float = resultSet.getFloat("discount_rate")
          var value = "" + pid + ":" + bid + ":" + cid + ":" + original_price + ":" + discount_price + ":" + sale_price + ":" + discount_rate
          hashMap.put(pid, value)
        }
        println("productMap>>>>>>>>>>>>>>" + hashMap.size)
        ctx.collect(hashMap)
        hashMap.clear()
        Thread.sleep(60000 * 5)
      }
    } catch {
      case ex: Exception => {
        logger.error("runException:{}", ex)
      }
    }
  }
}