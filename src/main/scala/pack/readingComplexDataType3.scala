package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readingComplexDataType3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").option("multiline", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/pets1.json")
    jsondf.show()
    jsondf.printSchema()

    val flatendf = jsondf.withColumn("Pets", expr("explode(Pets)"))
    flatendf.show()
    flatendf.printSchema()

  }

}
