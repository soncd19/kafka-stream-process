package com.cd.stream.base

import com.cd.stream.sink.SparkKafkaProducer
import com.cd.stream.source.{KafkaStreamConnector, RedisConnection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

abstract class BaseSparkKafkaStream(configPath: String) extends BaseApplication(configPath) {

  protected var sparkKafkaProducer: Broadcast[SparkKafkaProducer] = _
  protected var sparkRedisConnector: Broadcast[RedisConnection] = _

  override protected def run(sparkContext: SparkContext): Unit = {
    try {
      val streamingContext = new StreamingContext(sparkContext, Milliseconds.apply(500))
      val sparkConf = sparkContext.getConf
      val stream = KafkaStreamConnector.createStream(streamingContext, sparkContext.getConf)
      logInfo("created stream kafka")
      sparkKafkaProducer = sparkContext.broadcast(SparkKafkaProducer(sparkConf))
      logInfo("created broadcast sparkKafkaProducer")
      sparkRedisConnector = sparkContext.broadcast(RedisConnection(sparkConf))
      logInfo("created broadcast sparkRedisConnector")
      val maxCore = sparkConf.get("spark.cores.max").toInt
      logInfo("created Params")

      stream.foreachRDD { rdd => {
        if (!rdd.isEmpty()) {
          rdd.repartition(maxCore).foreachPartition {
            partitionOfRecords => execute(partitionOfRecords)
          }
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      }
      }
      logInfo("BaseStreamApplication/getStatus: StreamingContext Started")
      streamingContext.start()
      streamingContext.awaitTermination()
    }
    catch {
      case ex: Exception => logError(s"BaseStreamApplication/getException: ${ex.getMessage}")
      case t: Throwable => logError(s"BaseStreamApplication/getError: Sending message to kafka error: ${t.getMessage}")
    }
    finally {
      if (sparkKafkaProducer.value != null) {
        sparkKafkaProducer.value.close()
      }
      shutDown()
    }
  }

  protected def execute(records: Iterator[ConsumerRecord[String, String]]): Unit

}
