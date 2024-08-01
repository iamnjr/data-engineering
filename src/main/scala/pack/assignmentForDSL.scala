package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object assignmentForDSL {
  def main(args: Array[String]): Unit = {
    println("=====================Started=====================")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts","true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.format("csv").option("header","true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/dt.txt")
    df.show()

    println
    println("==================Select Expression usage=================")

    df.selectExpr("id",
      "tdate",
      "amount",
      "upper(category) as category",
      "product",
      "spendby").show()

    println("==================Select Expression usage=================")

    df.selectExpr("id",
      "tdate",
      "amount",
      "upper(category) as category",
      "lower(product) ",
      "spendby").show()
  }


}
