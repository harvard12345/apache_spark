package com.example.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object YoutubeDataAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Youtube Data Analysis")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val jsonData = spark.read.
      option("multiline", "true").
      json("D:/Data/youtube/CA_category_id.json")

    import spark.implicits._

    val categories = jsonData.
      select($"items.id", $"items.snippet.title").
      flatMap(myParser).
      select($"id".as("category_id"), $"title".as("category_title"))

    val csvData = spark.read.
      option("inferSchema", "true").
      option("header", "true").
      csv("D:/Data/youtube/*videos.csv")

    val videos = csvData.select(
      $"category_id".cast(IntegerType),
      $"views".cast(IntegerType),
      $"likes".cast(IntegerType),
      $"dislikes".cast(IntegerType),
      $"comment_count".as("comments").cast(IntegerType)).
      na.
      drop()

    val aggregated = videos.
      groupBy($"category_id").
      agg(
        "views" -> "sum",
        "likes" -> "sum",
        "dislikes" -> "sum",
        "category_id" -> "count",
        "comments" -> "sum").
      select(
        $"category_id",
        $"sum(views)".as("sum_views"),
        $"sum(likes)".as("sum_likes"),
        $"sum(dislikes)".as("sum_dislikes"),
        $"count(category_id)".as("count_videos"),
        $"sum(comments)".as("sum_comments"))

    val joined = categories.join(aggregated, "category_id").
      select($"category_title",
        $"sum_views",
        $"sum_likes",
        $"sum_dislikes",
        $"count_videos",
        $"sum_comments").
      orderBy($"sum_views".desc)

    joined.show(false)


  }

  case class Category(id: Int, title: String)

  def myParser(row: Row): Array[Category] = {
    val ids = row.getAs[mutable.WrappedArray[String]](0)
    val titles = row.getAs[mutable.WrappedArray[String]](1)
    val size = ids.length
    val categories = new Array[Category](size)
    for (i <- 0 until size) {
      categories(i) = Category(id = ids(i).toInt, title = titles(i))
    }
    categories
  }

}
