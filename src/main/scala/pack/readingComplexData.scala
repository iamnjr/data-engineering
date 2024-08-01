package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readingComplexData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").option("multiline", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/pic.json")
    jsondf.show()

    jsondf.printSchema()

    println("============================Flattened Data==================================")

    val flattendf = jsondf.selectExpr("id",
      "image.height",
      "image.url",
      "image.width",
      "name",
      "type")

    flattendf.show()
    flattendf.printSchema()

    println("============================Generating Complex Data==========================")

    val gencomdf = flattendf.select(col("id"),
      struct(col("height"),
        col("url"),
        col("width")).as("Details"),
      struct(col("name"),
        col("type")).as("Otherdetails"))
  }



}
