package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object scenario_7 {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val conf = new SparkConf().setAppName("StudentsScoreRange").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    // Sample data
    val data = Seq(
      (90, 2),
      (60, 3),
      (70, 5),
      (80, 1)
    )

    val df = data.toDF("Range","Count")
    df.show()

    val windowSpec = Window.orderBy(col("Range").desc)

    val dfWithWindow = df.withColumn("Count",sum("Count").over(windowSpec))
    dfWithWindow.show()


  }

}
