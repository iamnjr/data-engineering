package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readingComplexDataType3_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").option("multiline", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/actorsj.json")
    jsondf.show()
    jsondf.printSchema()

    val explodedf = jsondf.withColumn("Actors", expr("explode(Actors)"))
    explodedf.show()
    explodedf.printSchema()

    val flatdf = explodedf.select(

      "Actors.Birthdate",
      "Actors.BornAt",
      "Actors.age",
      "Actors.hasChildren",
      "Actors.hasGreyHair",
      "Actors.name",
      "Actors.photo",
      "Actors.picture.large",
      "Actors.picture.medium",
      "Actors.picture.thumbnail",
      "Actors.weight",
      "Actors.wife",
      "country",
      "version"
    )

    flatdf.show()
    flatdf.printSchema()
  }

}
