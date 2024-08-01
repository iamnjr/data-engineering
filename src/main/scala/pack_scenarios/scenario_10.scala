package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object scenario_10 {
  def main(args: Array[String]): Unit={
    val conf = new SparkConf().setAppName("StudentsScoreRange").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val df1 = Seq(1,2,3).toDF("numbers")
    val df2 = Seq(2,3,4).toDF("numbers")

    val resultdf = df1.union(df2).distinct()
    resultdf.show()

  }

}
