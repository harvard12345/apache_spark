package com.example.demo

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object StatefulDataAggregations {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Stateful Data Demo")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val sampleDF = spark.readStream.format("rate").load().as[Rate]

    sampleDF.writeStream.
      outputMode(OutputMode.Append).
      format("console").
      trigger(Trigger.ProcessingTime("10 seconds")).
      option("truncate", "false")


    val grouped = sampleDF.groupByKey { rate => rate.value % 2 }

    grouped.mapGroupsWithState(
      (key: Long, values: Iterator[Rate], state: GroupState[Long]) => {
        val previousState = state.getOption.getOrElse(0L)
        val newState = previousState + values.size
        state.update(newState)
        Result(key, newState)
      }
    ).writeStream.
      outputMode(Update).
      format("console")

    val schema = StructType(Seq(
      StructField("userId", IntegerType, nullable = true),
      StructField("movieId", IntegerType, nullable = true),
      StructField("rating", IntegerType, nullable = true),
      StructField("rateTime", IntegerType, nullable = true)
    ))

    val ratings = spark.readStream.schema(schema).option("sep", "\t").csv("D:\\input").as[Rating]

    ratings.groupByKey(rating => rating.movieId).mapGroupsWithState(
      (key: Int, values: Iterator[Rating], state: GroupState[Int]) => {
        val previousState = state.getOption.getOrElse(0)
        val newState = previousState + values.map(x => x.rating).sum
        state.update(newState)
        Summary(key, newState)
      }
    ).writeStream.
      format("console").
      option("numRows", 1000).
      outputMode(OutputMode.Update)

    import org.apache.spark.sql.functions._

    val signals = spark.readStream.
      format("rate").
      load().
      withColumn("deviceId", rint(rand() * 10).cast(IntegerType)).
      withColumn("value", $"value" % 10).
      as[Signal]

    val groupFunction: Signal => Long = {
      case Signal(_, deviceId, _) => deviceId
    }

    val groupedByKey = signals.groupByKey(groupFunction)

    groupedByKey.flatMapGroupsWithState(
      outputMode = OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(countValuesPerDevice).
      writeStream.
      outputMode(OutputMode.Append).
      option("truncate", value = false).
      format("console")

  }

  def countValuesPerDevice(key: Long, values: Iterator[Signal], state: GroupState[Long]): Iterator[Event] = {
    val previousState = state.getOption.getOrElse(0L)
    val newState = previousState + values.size
    state.update(newState)
    Iterator(Event(key, newState))
  }

  case class Event(deviceId: Long, count: Long)

  case class Rate(timestamp: Timestamp, value: Long)

  case class Result(key: Long, count: Long)

  case class Rating(userId: Int, movieId: Int, rating: Int, rateTime: Int)

  case class Summary(movieId: Int, sumOfRatings: Int)

  case class Signal(timestamp: Timestamp, deviceId: Long, value: Long)


}
