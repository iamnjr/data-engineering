package pack_scenarios

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object scenario_1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      ("sree_ramesh", 100),
      ("chiran_tan", 200),
      ("ram_krish", 300),
      ("john_stan", 400),
      ("mar_jany", 200)
    )

    // Create DataFrame
    val df = data.toDF("name", "sal")

    // Split the 'name' column into parts on the basis of '_' and add a new column for the part after '_'
    val dfWithSplit = df.withColumn("name_split",split(col("name"),"_")(1))

    dfWithSplit.show()

    // Sort the DataFrame based on the 'name_split' column
    val dfSorted = dfWithSplit.sort(col("name_split"))

    // Show the result
    dfSorted.show()
  }

}
