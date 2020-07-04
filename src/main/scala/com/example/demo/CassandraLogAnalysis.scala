package com.example.demo

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CassandraLogAnalysis {

  case class Log(level: String, thread: String, date: String, javaClass: String, lineNumber: Int, message: String)

  val regexPattern: Pattern = Pattern.compile("(\\w+)\\s+\\[(.+?)\\] (.+?) (.+?) (.+?):(\\d{1,9}) [-] (.+?)")

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Cassandra Log Analysis")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val logs = spark.read.textFile("D:/cassandra/logs/debug.log").map { line: String => {
      val matcher = regexPattern.matcher(line)
      if (matcher.matches())
        Log(
          matcher.group(1),
          matcher.group(2),
          matcher.group(3) + " " + matcher.group(4),
          matcher.group(5),
          matcher.group(6).toInt,
          matcher.group(7)
        )
      else
        Log("empty", "empty", "empty", "empty", 0, "empty")
    }
    }
      .where(s"message != 'empty'")

    val grouped = logs.groupBy($"thread", $"javaClass").count()

    val sorted = grouped.sort($"count".desc)

    sorted.show(100, truncate = false)
  }
}
