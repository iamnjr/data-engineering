package pack
import org.apache.spark._

object readingRDDFile {
  def main(args: Array[String]): Unit = {

    println("=================Started=====================")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.hostname", "localhost")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/datatxns") // change according your drive

    data.foreach(println)


  }

}
