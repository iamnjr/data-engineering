package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object convertingToDataFrame {

  case class schema(id : Double, category :String ,product :String)

  def main(args:Array[String]):Unit = {
    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._


    val data = sc.textFile("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/datatxns", 1) // change according to your path

    data.foreach(println)


    println
    println("===Column Filter===")
    println


    val split = data.map(x => x.split(","))

    val schemardd = split.map(x => schema(x(0).toDouble, x(1), x(2)))

    val filrdd = schemardd.filter(x => x.product.contains("Gymnastics"))

    filrdd.foreach(println)

    println
    println("===df===")
    println


    val df = filrdd.toDF()

    df.show()


  }
}
