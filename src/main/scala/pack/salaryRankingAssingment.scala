package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


import scala.io.{Codec, Source}

object salaryRankingAssingment {
  def main(args: Array[String]): Unit={

    val conf = new SparkConf().setAppName("app").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val depdf = spark.read.format("csv").option("header","true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/dept.csv")

    depdf.show()

    val windowSpec = Window.partitionBy("accountNumber").orderBy(col("salary").desc)

    // Add a rank column
    val rankedDf = depdf.withColumn("rank", rank().over(windowSpec))

    // Filter for the second highest salary in each department
    val secondHighestSalaryDf = rankedDf.filter(col("rank") === 2)

    // Select only accountNumber and salary columns
    val resultDf = secondHighestSalaryDf.select("accountNumber", "salary")

    // Show the result
    resultDf.show()


  }

}
