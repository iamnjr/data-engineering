package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataFrameAssignment1 {
  def main(args:Array[String]):Unit= {

    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val orcdf = spark.read.format("orc").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/orcfile.orc")

    orcdf.show()

    orcdf.createOrReplaceTempView("orcdf")

    spark.sql("select * from orcdf where age > 10").show()





  }

}
