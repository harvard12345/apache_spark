package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
object SparkStreamingElasticsearch {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Spark Elasticsearch Demo")


    val schema = StructType(Seq(
      StructField("maker", StringType, nullable = true),
      StructField("model", StringType, nullable = true),
      StructField("mileage", IntegerType, nullable = true),
      StructField("manufacture_year", StringType, nullable = true),
      StructField("engine_displacement", StringType, nullable = true),
      StructField("engine_power", IntegerType, nullable = true),
      StructField("body_type", StringType, nullable = true),
      StructField("color_slug", StringType, nullable = true),
      StructField("sth_year", StringType, nullable = true),
      StructField("transmission", StringType, nullable = true),
      StructField("door_count", IntegerType, nullable = true),
      StructField("seat_count", IntegerType, nullable = true),
      StructField("fuel_type", StringType, nullable = true),
      StructField("date_created", StringType, nullable = true),
      StructField("date_last_seen", StringType, nullable = true),
      StructField("price_eur", DoubleType, nullable = true)
    ))

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val cars = spark.readStream.
      schema(schema) csv "D:\\Data\\Cars\\cars.csv"

    import spark.implicits._


    cars.writeStream.foreachBatch { (batchDF: DataFrame, batchID: Long) => {
      batchDF.
        groupBy($"maker", $"fuel_type").
        agg("price_eur" -> "avg", "*" -> "count").
        select(
          $"maker",
          $"fuel_type",
          $"avg(price_eur)".as("average price"),
          $"count(1)".as("count")).
        orderBy($"average price".desc).
        write.
        format("org.elasticsearch.spark.sql").
        option("es.nodes", "localhost").
        option("es.port", "9200").
        option("es.resource", "cars").
        mode(SaveMode.Overwrite).
        save()
    }
    }.start().
      awaitTermination()


    /*cars.
      groupBy($"maker", $"fuel_type").
      agg("price_eur" -> "avg",
        "*" -> "count").select(
      $"maker",
      $"fuel_type",
      $"avg(price_eur)".as("average price"),
      $"count(1)".as("count")).
      orderBy($"average price".desc).
      writeStream.
      format("console").
      outputMode("complete").
      start().
      awaitTermination()*/
  }

}
