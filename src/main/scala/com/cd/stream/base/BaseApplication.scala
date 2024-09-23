package com.cd.stream.base

import com.cd.stream.utils.{Constants, Logging}
import com.cd.stream.utils.PropertiesFileReader.readConfig
import org.apache.spark.SparkContext

abstract class BaseApplication(configPath: String) extends SparkSessionWrapper with Logging {

  loadConfig(configPath)

  def start(): Unit = {
    logInfo("spark base application starting")
    run(spark.sparkContext)
  }

  protected def shutDown(): Unit = {
    spark.stop()
  }

  protected def CloseDown(): Unit = {
    spark.close()
  }

  protected def run(sparkContext: SparkContext): Unit

  private def loadConfig(configPath: String): Unit = {
    val properties = readConfig(configPath)
    logInfo("spark loading properties config")
    val propertiesNames = properties.propertyNames()
    while (propertiesNames.hasMoreElements) {
      val propertiesName = propertiesNames.nextElement().toString
      Constants.config += (propertiesName -> properties.getProperty(propertiesName))
    }
  }

}
