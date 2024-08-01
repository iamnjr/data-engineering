package pack_practice

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object practice_1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("practice").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) //RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/data-M-F5I_k4gu22GAFggbY_I.csv")

    df.show()

    df.printSchema()

    println("==================== Dropped nulls==================")

    val clean_df = df.na.drop()

    //clean_df.show()

    val standardized_phone = clean_df.withColumn("phone", regexp_replace(col("phone"), "[\\-1]", ""))

    //standardized_phone.show()

    val clean_email = standardized_phone.withColumn("email", trim(col("email")))

    //clean_email.show()

    val standardizedPostal = clean_email.withColumn("postalZip",regexp_replace(col("postalZip"),"-",""))

    standardizedPostal.show()

    val unique_countries = clean_df.select("country").distinct().count()



  }

}
