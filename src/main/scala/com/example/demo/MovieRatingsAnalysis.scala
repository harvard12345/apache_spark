package com.example.demo



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieRatingsAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Movie Ratings Analysis")
    conf.set("spark.sql.warehouse.dir", "C:\\Users\\harvard\\IdeaProjects\\Apache Spark\\spark-warehouse")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    val ratings = spark.read.
      format("csv").
      option("inferSchema", "true").
      option("sep", "\t").
      load("D:/Data/Movielens/u.data").
      select($"_c0".as("userId"), $"_c1".as("movieId"), $"_c2".as("rating"))

    val grouped = ratings.
      groupBy($"movieId").
      agg("rating" -> "count", "rating" -> "avg")

    val filtered = grouped.
      select($"movieId", $"count(rating)".as("count"), $"avg(rating)".as("average")).
      filter("average >= 4")

    val ordered = filtered.orderBy($"average".desc)

    ordered.write.saveAsTable("result")

    // for saving dataframe to mysql
    // val properties = new Properties()
    // properties.put("password", "harvard")
    // properties.put("user", "root")
    // instructors.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/madison", "instructors", properties)


  }

}
