package pack_task

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

object Task_6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val taskdf = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/tasks/task6.csv")
    taskdf.show()

    val ColumnsWithNulls = taskdf.columns.filter(colName => taskdf.filter(col(colName).isNull).count() > 0)

    println("Column name with Null values")
    ColumnsWithNulls.foreach(println)

    val filledDF = taskdf.withColumn("name",when(col("name").isNull,lit("default_value")).otherwise(col("name")))
    filledDF.show()

    filledDF.filter(col("age").isNotNull).show()

  }
}
