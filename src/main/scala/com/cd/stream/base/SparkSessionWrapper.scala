package com.cd.stream.base

import com.cd.stream.utils.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    val sparkConf = new SparkConf()
    for ((k, v) <- Constants.config) {
      sparkConf.set(k, v)
    }
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
  }

  protected def queryTable(sql: String): Dataset[Row] = {
    spark.read.format("jdbc")
      .option("url", spark.conf.get("datasourcedb.uri"))
      .option("driver", spark.conf.get("datasourcedb.driver"))
      .option("user", spark.conf.get("datasourcedb.user"))
      .option("password", spark.conf.get("datasourcedb.password"))
      .option("query", sql)
      .option("batchsize", "10000")
      .load()
  }


  protected def loadTable(table: String): Dataset[Row] = {
    spark.read
      .format("jdbc")
      .option("url", spark.conf.get("ddatasourcedb.uri"))
      .option("dbtable", table)
      .option("user", spark.conf.get("datasourcedb.user"))
      .option("password", spark.conf.get("datasourcedb.password"))
      .option("batchsize", "10000")
      .load()
  }

  protected def readJsonData(folderPath: String): Dataset[Row] = {
    spark
      .read
      .json(folderPath)
  }

  protected def readCSV(folderPath: String): Dataset[Row] = {
    spark
      .read
      .option("header", "true")
      .csv(folderPath)
  }

  protected def readParquetData(folderPath: String): Dataset[Row] = {
    spark
      .read
      .parquet(folderPath)
  }

}
