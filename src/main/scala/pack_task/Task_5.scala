package pack_task

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Task_5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val taskdf_1 = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/tasks/task5a.csv")
    taskdf_1.show()

    val taskdf_2 = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/tasks/task5b.csv")
    taskdf_2.show()

    println("Inner Join :")

    taskdf_1.join(taskdf_2,Seq("customer_id"),"inner").show()

  }
}
