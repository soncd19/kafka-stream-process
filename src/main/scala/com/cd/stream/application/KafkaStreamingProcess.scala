package com.cd.stream.application

import com.cd.stream.base.BaseSparkKafkaStream
import com.google.gson
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaStreamingProcess(configPath: String) extends BaseSparkKafkaStream(configPath) {

  override protected def execute(records: Iterator[ConsumerRecord[String, String]]): Unit = {

    records.foreach(record => {
      try {
        val topicName = record.topic()
        logInfo(s"streaming with topic name $topicName")
        val message = record.value()
        val jsonObject = new gson.JsonParser().parse(message).getAsJsonObject
        val id = jsonObject.get("id").toString
        val name = jsonObject.get("name").toString
        sparkRedisConnector.value.set(id, name)

      }
      catch {
        case ex: Exception => logError(s"EDTStreamFilterProcess/getException/${record.topic()}: ${ex.getMessage}")

      }
    })
  }


}