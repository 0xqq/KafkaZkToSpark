package kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.util.Random

/**
  * Created by Administrator on 2016/11/18.
  */
object KafkaEventProducer {
  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")
  private val random = new Random()

  private var pointer = -1


  private val NAME = Array[String]("ROLE001","ROLE002","ROLE003","ROLE004","ROLE005")
  private val MAX_USER_AGE = 60

  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  def click() : Double = {
    random.nextInt(10)
  }


  def main(args: Array[String]): Unit = {
    val topic = "sparkStreaming"
    val brokers = "172.20.1.104:9092,172.20.1.105:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "com.SimplePartitioner")


    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    val rand = new Random()
    var event = ""
    var num = 1
    while(true) {
      // prepare event data
      num = num + 1

/*
       发送json数据
       val event = new JSONObject()
        event
        .put("uid", getUserID)
        .put("event_time", System.currentTimeMillis.toString)
        .put("os_type", "pc")
        //.put("click_count", click)
        .put("click_count", num)*/

      //发送简单字符串

      //nama
      var nameIndex:Int = rand.nextInt(NAME.length)
      var name = NAME(nameIndex)

      //age
      var age = rand.nextInt(MAX_USER_AGE)
      if (age < 10) {
        age = age + 10
      }

      event += num + "/" + name + "/" +  age

      // produce event message
      producer.send(new KeyedMessage[String, String](topic,num.toString,event.toString))

      println("Message sent: " + event.toString)
      event = ""
      Thread.sleep(100)
    }
  }
}
