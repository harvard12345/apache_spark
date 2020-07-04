package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

object AccessLogAnalysis {

  case class LogLine(time: String, ipAddress: String, url: String, method: String, respCode: Int, respTime: Int)

  import java.util.regex.Pattern

  val regexPattern: Pattern = Pattern.compile("(\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}) [-] [-] (\\[.+?]) \"(\\S+) (.+?)\" (\\d{1,3}) (\\d{1,9})")

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Access Log Analysis")
    conf.set("spark.cassandra.connection.host", "localhost")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("schema", StructType(Seq(
        StructField("type", StringType, nullable = true),
        StructField("optional", BooleanType, nullable = true)
      )), nullable = true),
      StructField("payload", StringType, nullable = true)
    ))

    val messages = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "connect-test").
      load()

    import org.apache.spark.sql.functions._

    messages.writeStream.foreachBatch { (batchDF: DataFrame, batchID: Long) => {
      val logs = batchDF.select(from_json($"value".cast(StringType), schema)).
        select($"jsontostructs(CAST(value AS STRING))".as("simple")).
        select($"simple.payload").
        map(mapper).
        where(s"url != ''")
      val grouped = logs.groupBy($"url").count()
      val ordered = grouped.orderBy($"count".desc)

      ordered.show(false)

    }
    }.
      start().
      awaitTermination()
  }

  def mapper(line: Row): LogLine = {
    val matcher = regexPattern.matcher(line.getString(0))
    if (matcher.matches())
      LogLine(matcher.group(2),
        matcher.group(1),
        matcher.group(4),
        matcher.group(3),
        matcher.group(5).toInt,
        matcher.group(6).toInt)
    else
      LogLine("", "", "", "", 0, 0)
  }
}
