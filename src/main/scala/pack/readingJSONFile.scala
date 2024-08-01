package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object readingJSONFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").option("multiline", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/c.json")
    jsondf.show()

    jsondf.printSchema()

    val flattendf = jsondf.select("org",
      "trainer",
      "year",
      "zeyoAddress.permanent",
      "zeyoAddress.temporary",
      "zeyowork.doorno",
      "zeyowork.flat")

    flattendf.show()

    flattendf.printSchema()
  }

}
