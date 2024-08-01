package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, substring}

object mailMaskingScenario {
  def main(args: Array[String]): Unit = {
    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val mailorg = spark.read.format("csv").option("header", "true").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/mail.csv")
    mailorg.show()

    val maskedDF = mailorg
      .withColumn("mail", expr("substring(mail, 1, 1) || regexp_replace(substring(mail, locate('@', mail)), '[a-zA-Z0-9]', '*') || substring(mail, locate('@', mail) + 1)"))
      .withColumn("mob", expr("substring(mob, 1, 2) || '******' || substring(mob, length(mob) - 1, length(mob))"))

    maskedDF.show()


  }

}
