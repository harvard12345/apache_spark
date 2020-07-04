package com.example.demo

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

object WindowOperationsDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Demo")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val levels2 = Seq(
      ((2012, 12, 12, 12, 12, 0), 5),
      ((2012, 12, 12, 12, 12, 1), 5),
      ((2012, 12, 12, 12, 12, 2), 5),
      ((2012, 12, 12, 12, 12, 3), 5),
      ((2012, 12, 12, 12, 12, 4), 5),
      ((2012, 12, 12, 12, 12, 5), 5),
      ((2012, 12, 12, 12, 12, 6), 5),
      ((2012, 12, 12, 12, 12, 7), 5),
      ((2012, 12, 12, 12, 12, 8), 5),
      ((2012, 12, 12, 12, 12, 9), 5),
      ((2012, 12, 12, 12, 12, 10), 5),
      ((2012, 12, 12, 12, 12, 11), 5),
      ((2012, 12, 12, 12, 12, 12), 5),
      ((2012, 12, 12, 12, 12, 13), 9),
      ((2012, 12, 12, 12, 12, 14), 4),
      ((2012, 12, 12, 12, 12, 15), 10),
      ((2012, 12, 12, 12, 12, 16), 14),
      ((2012, 12, 12, 12, 12, 17), 14),
      ((2012, 12, 12, 12, 12, 18), 14),
      ((2012, 12, 12, 12, 12, 19), 14)
    ).map { case ((yy, mm, dd, h, m, s), a) => (LocalDateTime.of(yy, mm, dd, h, m, s), a) }.
      map { case (ts, a) => (Timestamp.valueOf(ts), a) }.
      toDF("timestamp", "level")

    import org.apache.spark.sql.functions._

    val levels1 = levels2.select($"timestamp", window($"timestamp", "3 seconds"), $"level")

    val levels3 = levels2.select($"timestamp",
      window($"timestamp",
        "3 seconds",
        "2 seconds"
      ),
      $"level")

    val levels4 = levels2.select($"timestamp",
      window($"timestamp",
        "3 seconds",
        "2 seconds",
        "1 seconds"
      ),
      $"level")


    levels2.groupBy(window($"timestamp", "5 seconds", "3 seconds")).
      agg(sum("level").as("level_sum"))

    val numGroups = spark.readStream.format("rate").load.as[(Timestamp, Long)]

    numGroups.groupByKey { case (time, value) => value % 2 }.
      mapGroups { case (group, values) => values.toList }.
      writeStream.
      format("console").
      outputMode("append").
      option("truncate", "false").
      trigger(Trigger.ProcessingTime("10 seconds"))

    import org.apache.spark.sql.functions._

    numGroups.groupBy(window($"timestamp", "10 seconds", "5 seconds")).
      agg(sum("value").as("sum_value")).
      orderBy($"window").
      writeStream.
      format("console").
      option("truncate", "false").
      option("numRows", 100).
      outputMode("complete")

    val df = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "test").
      load()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    df.select($"value".cast(StringType),
      $"timestamp").
      groupBy(window($"timestamp",
        "5 minutes", "2 minutes"), $"value").
      count().
      writeStream.
      option("truncate", "false").
      format("console").
      outputMode("complete")
  }

}
