package com.github.mrpowers.my.cool.project
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

object jsonReader extends App{
  val spark = SparkSession.builder().master( master = "local").getOrCreate()
  val filename = args(0)
  val sc = spark.sparkContext
  //val dataFile = sc.textFile("/home/admin/Downloads/winemag-data-130k-v2.json")
  val dataFile = sc.textFile(filename)

  case class MyclassOptions(     id:Option[Int],
                            country:Option[String],
                             points:Option[Int],
                              price:Option[Double],
                              title:Option[String],
                            variety:Option[String],
                             winery:Option[String])

    implicit val formats = DefaultFormats

  for (lines <- dataFile) {
    val id = (parse(lines).extract[MyclassOptions].id)
    val country = (parse(lines).extract[MyclassOptions].country)
    val points = (parse(lines).extract[MyclassOptions].points)
    val price = (parse(lines).extract[MyclassOptions].price)
    val title = (parse(lines).extract[MyclassOptions].title)
    val variety = (parse(lines).extract[MyclassOptions].variety)
    val winery = (parse(lines).extract[MyclassOptions].winery)

    println(s"[id:${id.getOrElse("Null")}] [country:${country.getOrElse("Null")}] [points:${points.getOrElse("Null")}] [price:${price.getOrElse("Null")}] [title:${title.getOrElse("Null")}] [variety:${variety.getOrElse("Null")}] [winery:${winery.getOrElse("Null")}]")
  }
}
