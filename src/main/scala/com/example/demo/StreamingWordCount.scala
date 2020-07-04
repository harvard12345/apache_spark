package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Streaming Word Count")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.readStream.text("D:/Data/input")

    import spark.implicits._

    val words = df.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy($"value").count()

    wordCounts.writeStream.format("console").outputMode("complete").start().awaitTermination()

  }

}
