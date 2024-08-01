package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readingDifferenttypeofFiles {
  case class schema(id:Int,category:String,product:String)
  def main(args:Array[String]):Unit= {

    println("===started===")
    println

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/devices.json")

    jsondf.show()

    val orcdf = spark.read.format("orc").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/orcfile.orc")

    orcdf.show()

    val pardf = spark.read.format("parquet").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/parfile.parquet")

    pardf.show()





    /*val avrodf = spark.read.format("avro").load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/part.avro")

    avrodf.show()

    avrodf.createOrReplaceTempView("avrodf")

    spark.sql("select * from avrodf").show()

    val filterdf = spark.sql("select * from avrodf where amount>10000")

    filterdf.show()

    filterdf.write.format("csv").option("header","true").mode("overwrite").save("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/newcsvwrite")

     */


  }

}
