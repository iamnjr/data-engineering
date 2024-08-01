package pack
import org.apache.spark._

object rddAssignment2 {

  def main(args: Array[String]): Unit = {
    println("=====================Started====================")

    val conf = new SparkConf().setAppName("second").setMaster("local[*]").set("spark.driver.hostname", "localhost")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/usdata", 1)
    data.take(10).foreach(println)

    val lendata = data.filter(x => x.length > 200)
    println
    println("===========================length > 200===================")
    lendata.foreach(println)

    val flatdata = lendata.flatMap(x => x.split(","))
    println
    println("===========================flatdata===================")
    flatdata.foreach(println)

    val repdata = flatdata.map(x => x.replace("-",""))
    println
    println("===========================Replace hyphon with nothing ===================")
    repdata.foreach(println)

    val condata = repdata.map(x => x + ",zeyo")
    println
    println("===========================Add zeyo to all===================")
    condata.foreach(println)





  }

}
