package com.aplum

import java.io.FileInputStream
import java.util.Properties

import com.sun.corba.se.impl.orb.ORBConfiguratorImpl.ConfigParser

/*
 *@Author: liuhang
 *
 *@CreateTime: 2019-07-26 11:12
 *
 */

class ReadConfig {
  private val properties = new Properties()
  def loadProperties():Properties = {
    val path: String = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
    properties.load(new FileInputStream(path))
    return properties
  }

}
