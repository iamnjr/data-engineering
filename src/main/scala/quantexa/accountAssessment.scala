package quantexa

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

// Define case classes for the structure of customer and account data
case class CustomerData(
                         customerId: String,
                         forename: String,
                         surname: String
                       )

case class AccountData(
                        customerId: String,
                        accountId: String,
                        balance: Long
                      )

case class CustomerAccountOutput(
                                  customerId: String,
                                  forename: String,
                                  surname: String,
                                  accounts: Seq[AccountData],
                                  numberAccounts: Int,
                                  totalBalance: Long,
                                  averageBalance: Double
                                )

object accountAssessment {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("quantexaAssessment1").setMaster("local[*]").set("spark.driver.host", "localhost")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() // DataFrame

    import spark.implicits._

    val inputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/SparkTestProject"
    val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

    // Load data from CSV files into Dataset
    val customerDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/customer_data.csv").as[CustomerData]
    val accountDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/account_data.csv")
      .withColumn("balance", 'balance.cast("long")).as[AccountData]

    customerDS.show()
    accountDS.show()

    // Perform left join to handle customers without accounts
    val joinDS = customerDS.joinWith(accountDS, customerDS("customerId") === accountDS("customerId"), "left_outer")
    joinDS.show(false)
    joinDS.printSchema()

    // Map to extract necessary fields, handling null account values
    val joinedWithFields = joinDS.map {
      case (customer, account) =>
        (
          customer.customerId,
          customer.forename,
          customer.surname,
          Option(account).map(_.accountId).getOrElse(null),
          Option(account).map(_.balance).getOrElse(0L)
        )
    }.toDF("customerId", "forename", "surname", "accountId", "balance")

    joinedWithFields.show(false)

    // Perform GroupBy and Aggregations
    val groupDF = joinedWithFields.groupBy("customerId", "forename", "surname")
      .agg(
        collect_list(struct("customerId", "accountId", "balance")).as("accounts"),
        count("accountId").as("numberAccounts"),
        sum("balance").as("totalBalance"),
        avg("balance").as("averageBalance")
      )
    groupDF.show(false)

    val groupDS = groupDF.map(row => CustomerAccountOutput(
      row.getAs[String]("customerId"),
      row.getAs[String]("forename"),
      row.getAs[String]("surname"),
      row.getAs[Seq[Row]]("accounts").map(a => AccountData(a.getString(0), a.getString(1), a.getLong(2))),
      row.getAs[Long]("numberAccounts").toInt,
      row.getAs[Long]("totalBalance"),
      row.getAs[Double]("averageBalance")
    ))

    groupDS.show(false)
    groupDS.printSchema()

    groupDS.write.mode("overwrite").parquet(outputPath + "assessment1.parquet")
  }
}
