package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object KafkaSparkCassandra {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Access Log Analysis")
    conf.set("spark.cassandra.connection.host", "localhost")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val accessLog = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "connect-test").
      load()

    import spark.implicits._

    accessLog.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) => {
      batchDF.
        select($"value".cast(StringType)).
        write.
        format("org.apache.spark.sql.cassandra").
        mode(SaveMode.Append).
        option("keyspace", "streams").
        option("table", "messages").
        option("spark.cassandra.auth.username", "harvard").
        option("spark.cassandra.auth.password", "harvard").
        save()
    }
    }.start().awaitTermination()
  }


}
