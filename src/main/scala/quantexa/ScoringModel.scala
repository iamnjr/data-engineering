package quantexa

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

// Define case classes
//case class AccountData(
//                        customerId: String,
//                        accountId: String,
//                        balance: Option[Long]
//                      )

//case class AddressData(
//                        addressId: String,
//                        customerId: String,
//                        address: String,
//                        number: Option[Int],
//                        road: Option[String],
//                        city: Option[String],
//                        country: Option[String]
//                      )

//case class CustomerDocument(
//                             customerId: String,
//                             forename: String,
//                             surname: String,
//                             accounts: Option[Seq[AccountData]],
//                             numberAccounts: Option[Int],
//                             totalBalance: Option[Long],
//                             averageBalance: Option[Double],
//                             address: Seq[AddressData]
//                           )

case class ScoringModel(
                         customerId: String,
                         forename: String,
                         surname: String,
                         accounts: Option[Seq[AccountData]],
                         numberAccounts: Option[Int],
                         totalBalance: Option[Long],
                         averageBalance: Option[Double],
                         address: Seq[AddressData],
                         linkToBVI: Boolean
                       )

object ScoringModelAssessment {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("quantexaAssessment3").setMaster("local[*]").set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.WARN)

    val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"
    // Load the CustomerDocument Dataset from Assessment 2
    val customerDocumentDS = spark.read.option("header", "true").parquet(outputPath + "assessment2.parquet").as[CustomerDocument]

    // Define BVI country check
    def hasBVIAddress(addresses: Seq[AddressData]): Boolean = {
      addresses.exists(_.country.contains("British Virgin Islands"))
    }

    // Map the CustomerDocument to ScoringModel, setting the linkToBVI flag
    val scoringModelDS: Dataset[ScoringModel] = customerDocumentDS.map { customer =>
      ScoringModel(
        customer.customerId,
        customer.forename,
        customer.surname,
        customer.accounts,
        customer.numberAccounts,
        customer.totalBalance,
        customer.averageBalance,
        customer.address,
        linkToBVI = hasBVIAddress(customer.address)
      )
    }

    scoringModelDS.show(false)

    // Count the number of customers linked to BVI
    val bviCount = scoringModelDS.filter(_.linkToBVI).count()
    println(s"Number of customers linked to BVI: $bviCount")
  }
}
