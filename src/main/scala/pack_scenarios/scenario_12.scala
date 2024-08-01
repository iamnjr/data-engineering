package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object scenario_12 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val tab = Seq(
      (1,"Spark"),
      (1,"Scala"),
      (1,"Hive"),
      (2,"Scala"),
      (3,"Spark"),
      (3,"Scala")
    ).toDF("id","subject")

    val df_collected_list = tab.groupBy("id").agg(collect_list("subject")).as("Subject").orderBy("id")

    df_collected_list.show()
  }

}
