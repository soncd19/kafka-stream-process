package com.cd.stream.source

import com.cd.stream.utils.Constants
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaStreamConnector {
  private val kafkaParams = scala.collection.mutable.Map[String, Object]()

  def createStream(streamingContext: StreamingContext, sparkConf: SparkConf): InputDStream[ConsumerRecord[String, String]] = {

    val topic1 = sparkConf.get(Constants.kafkaTopicsInCasa)
    val topic2 = sparkConf.get(Constants.kafkaTopicsInTopUp)

    val topics = List(topic1, topic2)

    kafkaParams += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> sparkConf.get(Constants.kafkaServer))
    kafkaParams += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    kafkaParams += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    kafkaParams += (ConsumerConfig.GROUP_ID_CONFIG -> sparkConf.get(Constants.kafkaGroupId))
    kafkaParams += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> sparkConf.get(Constants.autoOffsetReset))
    kafkaParams += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))
    kafkaParams += (ConsumerConfig.METADATA_MAX_AGE_CONFIG -> "5000")

    if ("true".equalsIgnoreCase(sparkConf.get(Constants.kafkaSecure))) {
      val secureProtocol: String = sparkConf.get(Constants.kafkaSecurityProtocol)
      val saslMechanism: String = sparkConf.get(Constants.kafkaSaslMechanism)
      val kafkaServiceName: String = sparkConf.get(Constants.kafkaServiceName)
      val user: String = sparkConf.get(Constants.kafkaUser)
      val pass: String = sparkConf.get(Constants.kafkaPassword)
      val stringBuilder = new StringBuilder()
      stringBuilder.append("org.apache.kafka.common.security.scram.ScramLoginModule" +
        " required serviceName=\"")
        .append(kafkaServiceName)
        .append("\" username=\"")
        .append(user)
        .append("\" password=\"")
        .append(pass).append("\";")
      kafkaParams += (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> secureProtocol)
      kafkaParams += (SaslConfigs.SASL_MECHANISM -> saslMechanism)
      kafkaParams += (SaslConfigs.SASL_JAAS_CONFIG -> stringBuilder.toString())
    }

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }
}
