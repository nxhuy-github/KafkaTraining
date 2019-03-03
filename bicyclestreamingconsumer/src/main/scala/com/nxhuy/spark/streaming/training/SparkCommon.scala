package com.nxhuy.spark.streaming.training

import org.apache.spark.SparkConf

object SparkCommon {
  val colBicycle = "bike_aggregation"

  val mongoUri = "mongodb://localhost:27017/streamdb." + colBicycle

  lazy val conf = new SparkConf()
  conf.setAppName("Bicycle-Streaming-Consumer")
    .setMaster("local[2]")
    .set("spark.mongodb.input.uri", mongoUri)
    .set("spark.mongodb.output.uri", mongoUri)

}
