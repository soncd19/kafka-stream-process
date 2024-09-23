package com.cd.stream.utils

object Constants {


  /* KafkaInput*/
  var kafkaServer = "kafka.server"
  var kafkaGroupId = "kafka.groupId"
  var autoOffsetReset = "kafka.auto.offset.reset"
  var kafkaSecure = "kafka.secure"
  var kafkaSecurityProtocol = "kafka.security.protocol"
  var kafkaSaslMechanism = "kafka.sasl.mechanism"
  var kafkaServiceName = "kafka.service.name"
  var kafkaUser = "kafka.user"
  var kafkaPassword = "kafka.password"
  var kafkaTopicsInCasa = "kafka.topics.in.casa"
  var kafkaTopicsInTopUp = "kafka.topics.in.topup"

  /*KafkaOutput*/
  var kafkaServer_Out = "kafka.server_out"
  var kafkaSecure_Out = "kafka.secure_out"
  var kafkaSecurityProtocol_Out = "kafka.security.protocol_out"
  var kafkaSaslMechanism_Out = "kafka.sasl.mechanism_out"
  var kafkaServiceName_Out = "kafka.service.name_out"
  var kafkaUser_Out = "kafka.user_out"
  var kafkaPassword_Out = "kafka.password_out"
  var kafkaClientId_Out = "kafka.clientId_out"

  var config: Map[String, String] = {
    Map()
  }

}
