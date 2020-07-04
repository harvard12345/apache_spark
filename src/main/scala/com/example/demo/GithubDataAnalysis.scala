package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source

object GithubDataAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Github Data Analysis")

    val spark = SparkSession.
      builder().
      config(conf).
      getOrCreate()

    val githubLog = spark.read.json("D:\\Data\\Github Commits\\2015-03-01-0.json")

    import spark.implicits._

    val pushes = githubLog.filter("type = 'PushEvent'")

    val grouped = pushes.groupBy($"actor.login").count()

    val ordered = grouped.orderBy($"count".desc)

    val employees = Set() ++ {
      for {
        line <- Source.fromFile("D:\\Data\\Github Commits\\ghEmployees.json").getLines()
      } yield line.trim
    }

    val bcEmployees = spark.sparkContext.broadcast(employees)

    val predicate = udf { user: String => bcEmployees.value.contains(user) }

    val filtered = ordered.filter(predicate($"login"))

    filtered.show()

  }

}
