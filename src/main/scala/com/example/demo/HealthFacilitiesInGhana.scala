package com.example.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HealthFacilitiesInGhana {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Health Facilities in Ghana")
    conf.setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val facilities = spark.read.option("inferSchema", "true").option("header", "true").csv("D:\\Data\\Health Facilities in Ghana\\health-facilities-gh.csv")

    import spark.implicits._

    val selected = facilities.select($"Region", $"District", $"Type", $"Ownership")

    val grouped = selected.groupBy($"Region", $"District", $"Type", $"Ownership").count()

    val sorted = grouped.sort($"Region", $"District")

    sorted.show(200, truncate = false)



  }
}
