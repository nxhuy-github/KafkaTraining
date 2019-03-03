package com.nxhuy.spark.streaming.training

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.spark.MongoSpark
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BicycleStreamingConsumer {
  case class BikeAggreration(bike_name: String, total: Int, data_time: String)

  var brokers = ""

  def main(args: Array[String]): Unit = {
    brokers = util.Try(args(0)).getOrElse("localhost:9092")

    val outTopic = util.Try(args(1)).getOrElse("bike-data")

    val batchDuration = util.Try(args(2)).getOrElse("30").toInt

    // Create streaming context from Spark
    val streamCtx = new StreamingContext(SparkCommon.conf, Seconds(batchDuration))
    val sparkCtx = streamCtx.sparkContext

    // Create SqlContext for using MongoDB
    lazy val sqlCtx = new SQLContext(sparkCtx)
    import sqlCtx.implicits._

    // Create direct Kafka Streaming for Consumer
    val outTopicSet = Set(outTopic)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val msg = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamCtx,
      kafkaParams,
      outTopicSet
    )

    /*----Code for process received data here---*/
    val value = msg.map(_._2)

    val bicycles = value.map(x => x.split(",")(1))

    val bicyclesDStream = bicycles.map(bike => Tuple2(bike, 1))
    val aggregatedBicycles = bicyclesDStream.reduceByKey(_+_)

    aggregatedBicycles.print()

    aggregatedBicycles.foreachRDD(rdd => {
      val today = Calendar.getInstance.getTime
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val data = rdd.map(
        x => BikeAggreration(x._1, x._2, formatter.format(today))
      )

      MongoSpark.save(data.toDF().write.mode(SaveMode.Append))
    })

    streamCtx.start()
    streamCtx.awaitTermination()
  }

}
