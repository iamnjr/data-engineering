package pack_scenarios

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object scenario_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      (10, 10, 600, "10-Jan-19"),
      (20, 10, 200, "10-Jun-19"),
      (30, 20, 300, "20-Jan-20"),
      (40, 30, 400, "30-Jun-20")
    )

    val df = data.toDF("emp_id", "dept_id", "salary", "hire_date")
    df.show()

    val dfWithDate = df.withColumn("hire_date", to_date(col("hire_date"), "dd-MMM-yy"))
    dfWithDate.show()

    val windowspec = Window.partitionBy("dept_id").orderBy("hire_date")

    val dfWithWindow = df.withColumn("rank", rank().over(windowspec))

    dfWithWindow.show()

    val dfFilter = dfWithWindow.filter(col("rank") === 1).drop("rank")
    dfFilter.show()


  }

}
