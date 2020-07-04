package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CrimeRatesInBoston {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Crimes in Boston")
    conf.set("spark.sql.warehouse.dir", "C:\\Users\\harvard\\IdeaProjects\\Apache Spark\\spark-warehouse")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val reading = true

    if (!reading) {
      val csvData1 = spark.read.option("inferSchema", "true").option("header", "true").csv("D:/Data/crimes in boston/crime.csv")

      val csvData2 = spark.read.option("inferSchema", "true").option("header", "true").csv("D:/Data/crimes in boston/offense_codes.csv")

      import spark.implicits._

      val codes = csvData2.select($"CODE".as("OFFENSE_CODE"), $"NAME").dropDuplicates("OFFENSE_CODE")

      val crimes = csvData1.select($"OFFENSE_CODE", $"DISTRICT", $"STREET").na.drop()

      val countByDistrictArray = crimes.groupBy($"DISTRICT").count().collect()

      var countByDistrictMap = Map[String, Long]()

      for (i <- countByDistrictArray) {
        countByDistrictMap += (i.getString(0) -> i.getLong(1))
      }

      val broadcastVariable = spark.sparkContext.broadcast(countByDistrictMap)

      import org.apache.spark.sql.functions._

      val myUDF = udf { district: String => {
        val total = broadcastVariable.value.get(district)
        if (total.isEmpty) 0 else total.get
      }
      }

      val aggregation = crimes.groupBy($"DISTRICT", $"STREET", $"OFFENSE_CODE").count().withColumn("ratio", $"count" / myUDF($"DISTRICT") * 100)

      val joined = codes.join(aggregation, "OFFENSE_CODE")

      val transformed = joined.select($"DISTRICT", $"STREET", $"NAME", $"count".as("COUNT"), $"ratio").sort($"DISTRICT", $"ratio".desc)

      transformed.write.partitionBy("DISTRICT").saveAsTable("crime_rates")
    } else {
      spark.sql("select * from crime_rates limit 10").show(false)
    }


  }

}
