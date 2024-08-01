package pack

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readingRDDFileAssignment {

  case class columns(id:String,category:String,product:String)

  def main(args:  Array[String]): Unit = {
    println("=====================Started====================")

    val conf = new SparkConf().setAppName("second").setMaster("local[*]").set("spark.driver.hostname", "localhost")


    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/datatxns",1)
    data.foreach(println)

    val split = data.map(x => x.split(","))

    val schemardd = split.map(x => columns(x(0),x(1),x(2)))

    val filrdd = schemardd.filter(x => x.product.contains("Gymnastics"))

    println
    println
    filrdd.foreach(println)






  }

}
