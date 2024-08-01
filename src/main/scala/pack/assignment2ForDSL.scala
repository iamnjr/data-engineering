package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object assignment2ForDSL {
  def main(args: Array[String]): Unit = {
    println("=====================Started=====================")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/dt.txt")
    df.show()

    println
    println("==================Filter spendby do not contain cash=================")

    df.filter(! (col("spendby")==="cash")).show()

    df.createOrReplaceTempView("df")

    println("===================ATTACH a column as status with Value 'zeyo'================")

    spark.sql("Select *,'zeyo' as status from df").show()

  }


}
