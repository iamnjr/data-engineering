package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object scenario_16 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("a", "english", 99),
      ("b", "english", 99),
      ("b", "maths", 100), // Assuming you have marks for all records
      ("c", "science", 70),
      ("c", "english", 98)
    ).toDF("student_name", "subject", "marks")

    data.show()

    val averageMarks = data.agg(avg("marks")).as("Avg marks").collect()(0)(0).asInstanceOf[Double]


    val aboveAverageStudents = data.filter(col("marks") > averageMarks)

    aboveAverageStudents.show()


  }

}
