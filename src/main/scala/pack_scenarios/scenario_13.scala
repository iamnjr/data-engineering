package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object scenario_13 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val inputData = Seq(
      (101, "Eng", 90),
      (101, "Sci", 80),
      (101, "Mat", 95),
      (102, "Eng", 75),
      (102, "Sci", 85),
      (102, "Mat", 90)
    ).toDF("Id", "subject", "marks")

    inputData.show()

    val df = inputData.groupBy("Id").pivot("subject").agg(first("marks"))
    df.show()


  }

}
