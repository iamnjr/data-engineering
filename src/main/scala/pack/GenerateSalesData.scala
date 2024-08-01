package pack

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object GenerateSalesData {

  def main(args: Array[String]): Unit = {


    // Create a Spark session
    val spark = SparkSession.builder.appName("DataGeneration").master("local").getOrCreate()

    // Define the schema for the DataFrame
    val schema = StructType(
      List(
        StructField("id", StringType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false),
        StructField("salary", DoubleType, nullable = false),
        StructField("department", StringType, nullable = false)
      )
    )

    // Generate data
    val data = (1 to 50).map { _ =>
      val id = java.util.UUID.randomUUID().toString
      val name = s"Person_$id"
      val age = scala.util.Random.nextInt(40) + 20
      val city = if (scala.util.Random.nextBoolean()) "CityA" else "CityB"
      val salary = 50000 + scala.util.Random.nextDouble() * 50000
      val department = if (scala.util.Random.nextBoolean()) "IT" else "Finance"
      Row(id, name, age, city, salary, department)
    }

    // Create a DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
    df.show()

    // Write DataFrame to CSV
    val outputPath = "file:///Users/niranjankashyap/Downloads/Big Data downloads/Data/SalesData"
    df.write.option("header","true").csv(outputPath)

    // Stop the Spark session
    spark.stop()


  }
}
