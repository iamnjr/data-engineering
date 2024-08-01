package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object salesDataPractice {
  def main(args: Array[String]): Unit = {
    println("=======================Started===============================")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")


    // Create a Spark session
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/SalesData/part-00000-4b6770e8-9f46-4f13-b00f-a0277c8a98f6-c000.csv")
    df.show()

    df.createOrReplaceTempView("df")

    println("=====================Calculate the average age of employees===========================")
    spark.sql("select avg(age)as average_age from df").show()

    println("===================Find the total number of employees in each department===============")
    df.groupBy("department").agg(count("department").as("employee_count")).show()

    println("===================Identify the employee with the highest salary========================")
    df.groupBy("department").agg(max("salary")).show()
    df.agg(max("salary")).show()

    println("===================Determine the average salary in each city=============================")
    df.groupBy("city").agg(avg("salary")).show()

    println("==================Filter out employees who are younger than 30 years old=================")
    df.select(col("name")).filter(col("age") < 30).show()

    println("==================Calculate the total salary expenditure for each department=============")
    df.groupBy("department").agg(sum("salary")).as("Expenditure").show()

    println("===============Sort the employees based on their salary in descending order===============")
    df.orderBy(col("salary").desc).show()

    println("===============Calculate the percentage of employees in each department===================")
    df.groupBy("department")
      .agg(count("id").alias("total_employees"))
      .withColumn("percentage", (col("total_employees") / sum("total_employees").over()).multiply(100)).show()

    spark.stop()

  }

}
