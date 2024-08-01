package pack_task

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val taskdf = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/tasks/task2.csv")
    taskdf.show()

    println("Extract the day of the week from the 'timestamp' column.")
    val day_of_the_week = taskdf.withColumn("day of the week", dayofweek(col("timestamp")))
    day_of_the_week.show()

    println("Extract the month from the 'timestamp' column.")
    val month_extraction = day_of_the_week.withColumn("Month", month(col("timestamp")))
    month_extraction.show()

    println("Extract the Year from the 'timestamp' column.")
    val year_extraction = month_extraction.withColumn("Year", year(col("timestamp")))
    year_extraction.show()


  }

}
