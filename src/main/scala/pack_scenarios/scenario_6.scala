package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object scenario_6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StudentsScoreRange").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    var df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/allc.csv")

    df.columns.foreach(println)

    df.columns.foreach { colName =>
      df = df.withColumnRenamed(colName, colName.replace(" ", "_"))
    }

    // Show the result
    df.show()

  }

}
