package com.cd.stream.utils

import org.apache.commons.codec.binary.Base64
import com.google.gson.{JsonObject, JsonParser}

object StringUtils {
  def toJson(data: String): JsonObject = {
    new JsonParser().parse(data).getAsJsonObject
  }
  def decodeBase64Text(encodedString: String): String = {
    val decodedBytes = Base64.decodeBase64(encodedString)
    new String(decodedBytes)
  }
}
