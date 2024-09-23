package com.cd.stream.source

import com.cd.stream.utils.Logging
import com.msb.redis.api.DistributedMapCacheClient
import com.msb.redis.serializer.{StringSerializer, StringValueDeserializer}
import com.msb.redis.service.RedisDistributedMapCacheClientService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf

import java.io.{IOException, Serializable}
import java.nio.charset.StandardCharsets
import java.util

case class RedisConnection(sparkConf: SparkConf) extends Serializable with Logging {

  private lazy val redisMapCacheClient: DistributedMapCacheClient = init()

  @throws[IOException]
  def xAdd(record: ConsumerRecord[String, String], redisTopic: String): Unit = {
    val value: String = record.value
    val messageBody: util.Map[Array[Byte], Array[Byte]] = new util.HashMap[Array[Byte], Array[Byte]]
    messageBody.put("json_in".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))
    messageBody.put("value_ts".getBytes(StandardCharsets.UTF_8), String.valueOf(System.currentTimeMillis).getBytes(StandardCharsets.UTF_8))
    redisMapCacheClient.xAdd(redisTopic.getBytes(StandardCharsets.UTF_8), messageBody)
  }

  @throws[IOException]
  def publish(record: ConsumerRecord[String, String], channel: String): Unit = {
    val value: String = record.value
    val keySerializer = new StringSerializer
    val valueSerializer = new StringSerializer
    redisMapCacheClient.publish(channel, value, keySerializer, valueSerializer)
  }

  /* append data 2 Redis using add function */
  @throws[IOException]
  def add(key: String, value: String): Unit = {
    logInfo(s"RedisAdd/Action: Append KeyValue to Redis")
    val keySerializer = new StringSerializer
    val valueSerializer = new StringSerializer
    redisMapCacheClient.put(key, value, keySerializer, valueSerializer)
  }

  /*Overwrite data 2 Redis using set function*/
  @throws[IOException]
  def set(key: String, value: String): Unit = {
    /*logInfo("Overwrite KeyValue to Redis")*/
    try{
      val keySerializer = new StringSerializer
      val valueSerializer = new StringSerializer
      redisMapCacheClient.set(key, value, -1L, keySerializer, valueSerializer)
    } catch {
      case ex: Exception => logError(s"RedisSet/getError: ${ex.getMessage}")
      case _: Throwable =>logError("RedisSet/getError: Overwrite KeyValue to Redis Error")
    }
  }
  /*read data from Redis*/
  @throws[IOException]
  def get(key: String): String = {
    try{
      logInfo("RedisGet/Action: get value from Redis")
      val keySerializer = new StringSerializer
      val valueDeserializer = new StringValueDeserializer
      redisMapCacheClient.get(key, keySerializer, valueDeserializer)
    }catch {
      case ex: Exception => logError(s"RedisGet/getStatus: Redis Query Get Error ${ex.getMessage}"); throw ex
    }

  }

  def close(): Unit = {
    logInfo("RedisClose/SystemControl: Close redis connection")
    redisMapCacheClient.disable()
  }

  private def init(): RedisDistributedMapCacheClientService = {
    try {
      logInfo("RedisInit/getStatus: Created Redis Connection")
      new RedisDistributedMapCacheClientService(sparkConf)
    }catch {
      case ex: Exception => logError(s"RedisInit/getStatus: Connection Get Error ${ex.getMessage}") ;throw ex}
  }

}