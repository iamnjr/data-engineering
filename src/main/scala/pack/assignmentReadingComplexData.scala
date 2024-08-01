package pack

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.io.Codec


object assignmentReadingComplexData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._


    val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=3")(Codec.UTF8).mkString

    println(urldata)

    val df = spark.read.json(sc.parallelize(List(urldata)))
    df.show()
    df.printSchema()

    val explodedf = df.withColumn("results", expr("explode(results)"))
    explodedf.show()
    explodedf.printSchema()

    val flattendf = explodedf.selectExpr("nationality",
      "results.user.cell",
      "results.user.dob",
      "results.user.email",
      "results.user.gender",
      "results.user.location.city",
      "results.user.location.state",
      "results.user.location.street",
      "results.user.location.zip",
      "results.user.md5",
      "results.user.name.first",
      "results.user.name.last",
      "results.user.name.title",
      "results.user.password",
      "results.user.password",
      "results.user.phone",
      "results.user.picture.large",
      "results.user.picture.medium",
      "results.user.picture.thumbnail",
      "results.user.registered",
      "results.user.salt",
      "results.user.sha1",
      "results.user.sha256",
      "results.user.username",
      "seed",
      "version")

    flattendf.show()
    flattendf.printSchema()

  }

}
