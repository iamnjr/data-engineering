package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readingComplexData2 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").option("multiline", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/c2.json")
    jsondf.show()

    jsondf.printSchema()

    println("============================Flattened Data==================================")

    val flattendf = jsondf.withColumn("permanentaddress",expr("zeyoaddress.user.permanentAddress"))
      .withColumn("temporaryaddress",expr("zeyoaddress.user.temporaryAddress"))
      .drop("zeyoaddress")

    flattendf.show()
    flattendf.printSchema()

    println("==============================Generate Complex Data==========================")

    val gencomplexdf = flattendf.withColumn("ZeyoAddress",expr("struct(struct('Permanant_Address','Temporary_Address') as users)"))

    gencomplexdf.show()
    gencomplexdf.printSchema()



  }

}
