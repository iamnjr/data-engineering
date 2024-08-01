package pack_scenarios

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

//val alternateCase = udf((s: String) => {
//  s.zipWithIndex.map {
//    case (c, i) => if (i % 2 == 0) c.toUpper else c.toLower
//  }.mkString("")
//})

object scenario_8 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Scenario_8").setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val subdf = spark.read.format("csv").option("header", "true")
      .option("delimiter", "~")
      .load("file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/sub.txt")

    subdf.show()

    val df = subdf.withColumn("name", expr("explode(split(name,','))"))
    df.show()

    //val data = Seq("sahil sahoo").toDF("input")

   // val resultdf = data.withColumn("output",alternateCase(col("input")))

   // resultdf.show()

    val dfWithColumn = df.withColumn("col_1",monotonically_increasing_id())

    dfWithColumn.show()

    val tran_1 = dfWithColumn.withColumn("Col_2",col("col_1") / 5 )
    tran_1.show()

    val tran_2 = tran_1.withColumn("Col_2",col("Col_2").cast("Int") + 1)
    tran_2.show()

    val tran_3 = tran_2.groupBy("Col_2").agg(collect_list("name")).orderBy("Col_2").as("Name").drop("Col_2")
    tran_3.show()




  }

}
