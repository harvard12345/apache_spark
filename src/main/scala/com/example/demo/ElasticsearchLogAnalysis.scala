package com.example.demo

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ElasticsearchLogAnalysis {

  val regexPattern: Pattern = Pattern.compile("\\[(.+?)\\]\\[(.+?)\\s*\\]\\[(.+?)\\s*\\]\\s\\[(.+?)\\]\\s(.+)")

  case class LogLine(date: String, logLevel: String, javaClass: String, host: String, message: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Elasticsearch Log Analysis")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val logData = spark.read.textFile("D:\\elasticsearch\\logs\\elasticsearch-2019-07-16-1.log").map(mapper).where(s"date != 'empty'")

    val warnCount = logData.where(s"logLevel = 'WARN'").count()

    println(warnCount)

  }

  def mapper(line: String): LogLine = {
    val matcher = regexPattern.matcher(line)
    if (matcher.matches())
      LogLine(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        matcher.group(4),
        matcher.group(5))
    else
      LogLine("empty", "empty", "empty", "empty", "empty")
  }


}
