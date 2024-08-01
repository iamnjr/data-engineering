package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object scenarioBased {
  def main(args: Array[String]): Unit = {
    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    println("========================================Joins=============================")
    val srcfil = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/source.csv")
    srcfil.show()

    val tarfil = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/target.csv")
    tarfil.show()

    println("=========================================Full Join=========================")

    val fulljoin = srcfil.join(tarfil, Seq("id"), "full").orderBy("id")
    fulljoin.show()

    val com = fulljoin.withColumn("comments"
      , expr("case when sname=tname then 'Matched' else 'Mismatch' end"))

    com.show()

    val newcom = com.filter(!(col("comments") === "Matched"))
    newcom.show()

    val updcom = newcom.withColumn("comments", expr("case when tname is null then 'New is Source'" +
      " when sname is null then 'New in Target' else comments end"))

    updcom.show()

    println("===============================Final output==================================")

    updcom.select("id", "comments").show()
  }

}
