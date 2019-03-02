package com.nxhuy.kafka.training

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import scala.util.control.Breaks

object BicycleDataProducer {
  def main(args: Array[String]): Unit = {
    val brokers = util.Try(args(0)).getOrElse("localhost:9092")

    val topic = util.Try(args(1)).getOrElse("bike-data")

    val events = util.Try(args(2)).getOrElse("0").toInt

    val intervalEvent = util.Try(args(3)).getOrElse("30").toInt

    val rndStart = util.Try(args(4)).getOrElse("0").toInt

    val rndEnd = util.Try(args(5)).getOrElse("500").toInt

    val clientId = UUID.randomUUID().toString()

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("cliend.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    println("==========================BEGIN=======================")

    val bicycleSrc = List("Trek", "Giant", "Jett", "Cannondale", "Surly")
    val rnd = new Random()
    val rnd2 = new Random()

    var i = 0

    val loop = new Breaks()

    loop.breakable{
      while(true){
        val n = rndStart + rnd2.nextInt(rndEnd - rndStart + 1)
        for(i <- Range(0, n)){
          val today = Calendar.getInstance.getTime
          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val key = UUID.randomUUID().toString().split("-")(0)
          val value = formatter.format(today) + "," + bicycleSrc(rnd.nextInt(bicycleSrc.length))
          val data = new ProducerRecord[String, String](topic, key, value)

          println("----topic: " + topic + " ----")
          println("key: " + data.key())
          println(("value: " + data.value()))
          producer.send(data)
        }
        val k = i + 1
        println(s"--- #$k: $n records in [$rndStart, $rndEnd] ---")

        if(intervalEvent > 0)
          Thread.sleep(intervalEvent * 1000)

        i += 1
        if(events > 0 && i == events)
          loop.break()
      }
    }
  }
}

