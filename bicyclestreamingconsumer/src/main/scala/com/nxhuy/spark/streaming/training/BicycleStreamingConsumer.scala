package com.nxhuy.spark.streaming.training

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import com.google.gson.Gson
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies}
/*import kafka.serializer.StringDecoder*/
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BicycleStreamingConsumer {
  case class BikeAggreration(bike_name: String, total: Int, data_time: String)

  var brokers = ""
  var inTopic = ""

  def main(args: Array[String]): Unit = {
    brokers = util.Try(args(0)).getOrElse("localhost:9092")

    val outTopic = util.Try(args(1)).getOrElse("bike-data")

    inTopic = util.Try(args(2)).getOrElse("bike-data-visualization")

    val batchDuration = util.Try(args(2)).getOrElse("30").toInt

    val sparkSession = SparkSession.builder.config(SparkCommon.conf).getOrCreate()
    import sparkSession.implicits._

    // Create streaming context from Spark
    val streamCtx = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))
    val sparkCtx = streamCtx.sparkContext

    // Create SqlContext for using MongoDB
    // lazy val sqlCtx = new SQLContext(sparkCtx)
    // import sqlCtx.implicits._

    // Create direct Kafka Streaming for Consumer
    val outTopicSet = Set(outTopic)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "bicycle_data"
    )
    val preferredHosts = LocationStrategies.PreferConsistent

    val msg = KafkaUtils.createDirectStream[String, String](
      streamCtx,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](outTopicSet, kafkaParams)
    )

    /*----Code for process received data here---*/
    val value = msg.map(_.value())

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

      data.foreachPartition(pushBikeInfoInKafka)

    })

    /*----Code for process received data here---*/

    //Start the stream
    streamCtx.start()
    streamCtx.awaitTermination()
  }

  def pushBikeInfoInKafka(items: Iterator[BikeAggreration]): Unit = {

    //Create some properties
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", UUID.randomUUID().toString())
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)

    items.foreach( obj => {
      val key = UUID.randomUUID().toString().split("-")(0)
      val gson = new Gson()
      val value = gson.toJson(obj)

      val data = new ProducerRecord[String, String](inTopic, key, value)

      println("--- topic: " + inTopic + " ---")
      println("key: " + data.key())
      println("value: " + data.value() + "\n")

      producer.send(data)

    })
    producer.close()
  }

}
