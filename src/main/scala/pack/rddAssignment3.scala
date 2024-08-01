package pack

import org.apache.spark._

object rddAssignment3 {
  def main(args: Array[String]): Unit = {
    println("=====================Started====================")

    val conf = new SparkConf().setAppName("second").setMaster("local[*]").set("spark.driver.hostname", "localhost")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/usdata", 1)
    data.take(10).foreach(println)

    val ladata = data.filter(x => x.contains("LA"))
    println
    println("==========================LA data=======================")
    ladata.foreach(println)

    val flatdata = ladata.flatMap(x => x.split(","))
    println
    println("==========================Flatten LA data=======================")
    flatdata.foreach(println)

  }

}
