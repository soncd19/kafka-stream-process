package com.cd.stream

import com.cd.stream.application.KafkaStreamingProcess
import org.apache.log4j.{Level, Logger}

/**
 * @author soncd2
 */
object App {

  def main(args: Array[String]): Unit = {
    showCommand()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val command = if (args.length == 0) "KafkaStreamingProcess" else args(0)
    val configPath = if (args.length == 0) System.getProperty("user.dir") + "/config/application.properties" else args(1)

    try {
      command match {
        case "KafkaStreamingProcess" => new KafkaStreamingProcess(configPath).start()
      }
    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
    }

  }

  private def showCommand(): Unit = {
    val introduction =
      """
        |WELCOME TO SPARK STREAM PROCESS
        |- 1 argument: Spark application command
        |- 2 argument: Application configuration path
        |""".stripMargin

    println(introduction)
  }

}
