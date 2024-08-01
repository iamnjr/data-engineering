package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object scenario_15 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, "Henry", "henry12@gmail.com"),
      (2, "Smith", "smith@yahoo.com"),
      (3, "Martin", "martin221@hotmail.com")
    ).toDF("id", "name", "email")

    val df = data.withColumn("domain",split(col("email"),"@")(1)).drop("email")
    df.show()

  }

}
