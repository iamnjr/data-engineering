package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object usingDSL {
  def main(args: Array[String]): Unit = {
    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //Reading and Showing full table
    val df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/dt.txt")
    df.show()

    //Single filter
    df.filter(!(col("category") === "Exercise")).show()

    //Multi Filter
    df.filter(
      !(col("category") === "Exercise"
        &&
        col("spendby") === "cash"
        )).show()

    // Filter category = 'Exercise' or spendby='cash'
    df.filter(
      !(col("category") === "Exercise"
        ||
        col("spendby") === "cash"
        )).show()

    //Filtering values from same column (IN operation from SQL)
    df.filter(!(col("category") isin("Exercise", "Team Sports"))).show()

    println("======================Product is null====================")

    df.filter(!(col("Product").isNull)).show()

    println("======================Product is not null====================")

    df.filter(!(col("Product").isNotNull)).show()

    println("===============Using SelectExpr===============================")

    df.selectExpr("id",
      "tdate",
      "amount",
      "upper(category) as category",
      "product",
      "spendby",
      "case when spendby='cash' then 0 else 1 end as status"
    ).show()

    df.printSchema()

    df.withColumn("category", expr("upper(category)"))
      .withColumn("status", expr("case when spendby='cash' then 0 else 1 end"))
      .withColumn("new_amount", expr("1000+amount")).show()

    println("===================================Joins===============================")


    val srcfil = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/source.csv")
    srcfil.show()

    val tarfil = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/target.csv")
    tarfil.show()

    val innerJoinDF = srcfil.join(tarfil, Seq("id"), "inner")
    println("Inner Join:")
    innerJoinDF.show()

    val leftJoinDF = srcfil.join(tarfil, Seq("id"), "left")
    println("Left Outer Join:")
    leftJoinDF.show()

    val rightJoinDF = srcfil.join(tarfil, Seq("id"), "right")
    println("Right Outer Join:")
    rightJoinDF.show()

    val fullOuterJoinDF = srcfil.join(tarfil, Seq("id"), "full")
    println("Full Outer Join:")
    fullOuterJoinDF.show()

    val crossJoinDF = srcfil.crossJoin(tarfil)
    println("Cross Join:")
    crossJoinDF.show()

    println("===================================Using Aggregation==============================")
    val cust = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/agg.csv")
    cust.show()

    val aggdf = cust.groupBy("name").agg(
      sum("amt").cast(IntegerType).as("total"),
      count("amt").as("cnt")
    )

    aggdf.show()


  }

}
