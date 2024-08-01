package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object uberAssignment {
  def main(args: Array[String]): Unit = {
    println("=====================Started=====================")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/uber.txt")
    df.show()

    // Convert the date string to a date type
    val dfWithDate = df.withColumn("parsed_date", to_date(col("date"), "M/d/yyyy"))

    // Extract the day of the week
    val dfWithDayOfWeek = dfWithDate.withColumn("day_of_week", date_format(col("parsed_date"), "EEEE"))

    // Show the result
    dfWithDayOfWeek.show()

    println("===============================Using Aggregations===========================")

    dfWithDayOfWeek.agg(sum("trips").as("Total Sum of trips")).show()

    println("===============================Using Aggregations===========================")

    dfWithDayOfWeek.filter(col("day_of_week")==="Sunday" &&
    col("active_vehicles") > "300").agg(max("trips").as("Max of trips ")).show()

    dfWithDayOfWeek.createOrReplaceTempView("dfWithDayOfWeek")

    val sqlQuery =
      """
      SELECT MAX(trips) AS `Max of trips`
      FROM dfWithDayOfWeek
      WHERE day_of_week = 'Sunday' AND active_vehicles > 300
    """

    println("====================================Spark SQL===========================================")

    val result = spark.sql(sqlQuery)
    result.show()

    println("====================================DSL===========================================")

    val result1 = dfWithDayOfWeek
      .filter(col("day_of_week") === "Sunday" && col("active_vehicles") > 300)
      .agg(max("trips").as("Max of trips"))

    result1.show()


  }


}
