package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object scenario_14 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val mixedList = List(10, 5, 24, "Hi", 90, 12, "Hello")
    val mixedRDD = sc.parallelize(mixedList)
    mixedRDD.foreach(println)

    val numbersOnlyRDD = mixedRDD.filter {
      case number: Int => true
      case _ => false
    }

    val numbersOnly = numbersOnlyRDD.collect()
    println(numbersOnly.mkString(", "))
  }
}
