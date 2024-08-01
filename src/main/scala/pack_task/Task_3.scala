package pack_task

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Task_3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val taskdf = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/tasks/task3.csv")
    taskdf.show()

    println("Group the data by 'product_category'")
    val product_category = taskdf.groupBy("product_category").agg(sum("quantity").as("Quantity"))
    product_category.show()

    println("Calculate the total revenue for each category based on the 'quantity' and 'unit_price' columns.")
    val total_revenue = taskdf.withColumn("Total Revenue", expr(" quantity * unit_price"))
    total_revenue.groupBy("product_category").agg(sum("Total Revenue")).show()

  }

}
