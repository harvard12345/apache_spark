package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ResilientDistributedDataset {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Resilient Distributed Dataset")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val scc = new StreamingContext(spark.sparkContext, Seconds(20))

    val ratings = scc.textFileStream("D:\\input")

    ratings.
      map { line => line.split("\t") }.
      map { tokens => (tokens(1).toInt, tokens(2).toInt) }.
      reduceByKey((x, y) => x + y).
      repartition(1)

    scc.start()
    scc.awaitTermination()

    val df = spark.readStream.textFile("D:\\input")

  }

}
