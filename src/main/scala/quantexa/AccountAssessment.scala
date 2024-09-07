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
                        balance: Long
                      )

//Expected Output Format
case class CustomerAccountOutput(
                                  customerId: String,
                                  forename: String,
                                  surname: String,
                                  //Accounts for this customer
                                  accounts: Seq[AccountData],
                                  //Statistics of the accounts
                                  numberAccounts: Int,
                                  totalBalance: Long,
                                  averageBalance: Double
                                )

object AccountAssessment extends App {
  val conf = new SparkConf().setAppName("quantexaAssessment1").setMaster("local[*]").set("spark.driver.host", "localhost")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  Logger.getRootLogger.setLevel(Level.WARN)

  val inputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/SparkTestProject"
  val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

  // Load data from CSV files into Dataset
  val customerDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/customer_data.csv").as[CustomerData]
  val accountDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/account_data.csv").withColumn("balance", 'balance.cast("long")).as[AccountData]


  customerDS.show()
  accountDS.show()

  val groupedAccountsDS = accountDS.groupByKey(_.customerId)

  val finalAccountDS = groupedAccountsDS.mapGroups {
    (customerId, accountDataIter) =>
      val seqOfAccountData = accountDataIter.toSeq
      val numberAccounts = seqOfAccountData.size
      val totalBalance = seqOfAccountData.map(_.balance).sum
      val averageBalance = totalBalance.toDouble / numberAccounts.toDouble

      (customerId, seqOfAccountData, numberAccounts, totalBalance, averageBalance)
  }

      val customerAccountDS = customerDS.joinWith(
        finalAccountDS,
        customerDS("customerId") === finalAccountDS("_1"),
        "left"
      )

      val outputDS = customerAccountDS.map {
        case (customer, null) =>
          CustomerAccountOutput(
            customerId = customer.customerId,
            forename = customer.forename,
            surname = customer.surname,
            accounts = Seq.empty,
            numberAccounts = 0,
            totalBalance = 0,
            averageBalance = 0
          )
        case (customer, account) =>
          CustomerAccountOutput(
            customerId = customer.customerId,
            forename = customer.forename,
            surname = customer.surname,
            accounts = account._2,
            numberAccounts = account._3,
            totalBalance = account._4,
            averageBalance = account._5
          )
      }
  outputDS.show(false)
  outputDS.write.mode("overwrite").parquet(outputPath + "assessment1.parquet")
}
