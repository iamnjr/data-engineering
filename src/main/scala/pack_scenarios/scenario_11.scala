package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object scenario_11 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val tab_1 = Seq(
      (1, "Henry"),
      (2, "Smith"),
      (3, "Hall")
    ).toDF("id","Name")

    val tab_2 = Seq(
      (1, 100),
      (2, 500),
      (4, 1000)
    ).toDF("id","Salary")

    val joindf = tab_1.join(tab_2,Seq("id"),"left").withColumn("Salary",expr("coalesce(Salary,0)"))
    joindf.show()
  }

}
