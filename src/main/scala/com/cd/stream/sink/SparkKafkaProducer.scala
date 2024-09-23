package com.cd.stream.sink

import com.cd.stream.utils.{Constants, Logging}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}
import org.apache.spark.SparkConf

import java.io.Serializable
import java.nio.charset.Charset
import java.util.Properties

case class SparkKafkaProducer(sparkConf: SparkConf) extends Serializable with Logging {

  private lazy val producer: KafkaProducer[Array[Byte], Array[Byte]] = init()

  def send(message: String, topic: String): Unit = {
    try {
      val record = new ProducerRecord(topic, getKey(System.currentTimeMillis()), serialize(message))
      val future = producer.send(record, new DummyCallback())
      val recordMetadata = future.get()
      logInfo("future before sen message-> topic: " + recordMetadata.topic()
        + ", offset " + recordMetadata.offset() + ", partition" + recordMetadata.partition()
        + ",timestamp " + recordMetadata.timestamp())
    }
    catch {
      case ex: Exception => logError(ex.getMessage)
      case _: Throwable => logError("Sending message to kafka error")
    }
  }

  private def getKey(i: Long): Array[Byte] = {
    serialize(String.valueOf(i))
  }

  private def serialize(message: String) = {
    message.getBytes(Charset.defaultCharset())
  }

  private def init(): KafkaProducer[Array[Byte], Array[Byte]] = {
    val kafkaProperties: Properties = new Properties()
    logInfo("starting create kafka producer")
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sparkConf.get(Constants.kafkaServer_Out))
    logInfo("kafka bootstrap server {}" + sparkConf.get(Constants.kafkaServer_Out))
    kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, sparkConf.get(Constants.kafkaClientId_Out))
    kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "0")
    kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, "0")
    kafkaProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "20971520")
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "86016")
    kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    if ("true".equalsIgnoreCase(sparkConf.get(Constants.kafkaSecure_Out))) {
      logInfo("kafka secure is true")
      val secureProtocol = sparkConf.get(Constants.kafkaSecurityProtocol_Out)
      val saslMechanism = sparkConf.get(Constants.kafkaSaslMechanism_Out)
      val kafkaServiceName = sparkConf.get(Constants.kafkaServiceName_Out)
      val user = sparkConf.get(Constants.kafkaUser_Out)
      val pass = sparkConf.get(Constants.kafkaPassword_Out)
      val stringBuilder = new StringBuilder()
      stringBuilder.append("org.apache.kafka.common.security.scram.ScramLoginModule" +
        " required serviceName=\"")
        .append(kafkaServiceName)
        .append("\" username=\"")
        .append(user)
        .append("\" password=\"")
        .append(pass).append("\";")

      kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, secureProtocol)
      kafkaProperties.put(SaslConfigs.SASL_MECHANISM, saslMechanism)
      kafkaProperties.put(SaslConfigs.SASL_JAAS_CONFIG,stringBuilder.toString())
    }
    sys.addShutdownHook {
      close()
    }
    new KafkaProducer[Array[Byte], Array[Byte]](kafkaProperties)
  }

  def close(): Unit = producer.close()

  private class DummyCallback extends Callback {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        logError("Error while producing message to topic : " + recordMetadata.topic)
      }
      else {
        logDebug("sent message to topic, partition, offset: " + recordMetadata.topic + ", "
          + recordMetadata.partition + ", " + recordMetadata.offset)
      }
    }
  }


}
