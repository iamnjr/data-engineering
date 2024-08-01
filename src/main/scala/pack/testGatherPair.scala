package pack

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.io.Codec
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


object testGatherPair {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    def GetUrlContentJson(url: String): DataFrame = {
      val result = scala.io.Source.fromURL(url).mkString
      //only one line inputs are accepted. (I tested it with a complex Json and it worked)
      val jsonResponseOneLine = result.toString().stripLineEnd
      //You need an RDD to read it with spark.read.json! This took me some time. However it seems obvious now
      val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)

      val jsonDf = spark.read.json(jsonRdd)
      return jsonDf
    }

    val response = GetUrlContentJson("https://randomuser.me/api/0.8/?results=3")
    response.show


  }

}
