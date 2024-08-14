package quantexa

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.{Level, Logger}

case class CustomerData(
                         customerId: String,
                         forename: String,
                         surname: String
                       )

case class AccountData(
                        customerId: String,
                        accountId: String,
                        balance: Option[Long]
                      )

case class CustomerAccountOutput(
                                  customerId: String,
                                  forename: String,
                                  surname: String,
                                  accounts: Option[Seq[AccountData]],
                                  numberAccounts: Option[Int],
                                  totalBalance: Option[Long],
                                  averageBalance: Option[Double]
                                )

object AccountAssessment {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("quantexaAssessment1").setMaster("local[*]").set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.WARN)

    val inputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/SparkTestProject"
    val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

    // Load data from CSV files into Dataset
    val customerDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/customer_data.csv").as[CustomerData]
    val accountDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/account_data.csv").withColumn("balance",'balance.cast("long")).as[AccountData]


    customerDS.show()
    accountDS.show()

    val groupedAccountDS = accountDS
      .groupByKey(_.customerId)
      .mapGroups { case (customerId, accounts) =>
        val accountSeq = accounts.toSeq
        val totalBalance = accountSeq.flatMap(_.balance).sum
        val numberAccounts = accountSeq.length
        val averageBalance = if (numberAccounts > 0) totalBalance.toDouble / numberAccounts else 0.0
        (customerId, Option(accountSeq), Option(numberAccounts), Option(totalBalance), Option(averageBalance))
      }
      .toDF("customerId", "accounts", "numberAccounts", "totalBalance", "averageBalance")

    // Join with customer data
    val customerAccountOutputDS: Dataset[CustomerAccountOutput] = groupedAccountDS
      .join(customerDS, Seq("customerId"), "right_outer")
      .as[(String, Option[Seq[AccountData]], Option[Int], Option[Long], Option[Double], String, String)]
      .map {
        case (customerId, accounts, numberAccounts, totalBalance, averageBalance, forename, surname) =>
          CustomerAccountOutput(
            customerId,
            Option(forename).getOrElse("Unknown"), // Handle missing values
            Option(surname).getOrElse("Unknown"),
            accounts,
            numberAccounts.orElse(Some(0)), // Provide a default value for nulls
            totalBalance.orElse(Some(0L)), // Provide a default value for nulls
            averageBalance.orElse(Some(0.0)) // Provide a default value for nulls
          )
      }

    customerAccountOutputDS.show(false)
    customerAccountOutputDS.printSchema()

    customerAccountOutputDS.write.mode("overwrite").parquet(outputPath + "assessment1.parquet")

  }
}
