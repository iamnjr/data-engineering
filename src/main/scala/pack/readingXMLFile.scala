package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object readingXMLFile {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val taskdf = spark.read.format("xml").option("rowtag", "book").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/book.xml")
    taskdf.show()
  }

}
