package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DutchEnergyConsumption {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Dutch Energy Consumption")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val electricityData = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\Data\\Dutch Energy Consumption\\Electricity\\*.csv")
    val gasData = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\Data\\Dutch Energy Consumption\\Gas\\*.csv")

    val groupedElectricity = electricityData.groupBy($"city", $"street").agg("annual_consume" -> "sum")

    val selectedElectricity = groupedElectricity.select($"city", $"street", $"sum(annual_consume)".as("sum_electricity"))

    val groupedGas = gasData.groupBy($"city", $"street").agg("annual_consume" -> "sum")

    val selectedGas = groupedGas.select($"city", $"street", $"sum(annual_consume)".as("sum_gas"))

    val joined = selectedElectricity.join(selectedGas, Seq("city", "street"))

    val ordered = joined.orderBy($"city", $"street")

    ordered.show()

  }

}
