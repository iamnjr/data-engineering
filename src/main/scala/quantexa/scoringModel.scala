package quantexa

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

case class CustomerDocument(
                             customerId: String,
                             forename: String,
                             surname: String,
                             accounts: Seq[AccountData],
                             address: Seq[AddressData]
                           )

case class ScoringModel(
                         customerId: String,
                         forename: String,
                         surname: String,
                         accounts: Seq[AccountData],
                         address: Seq[AddressData],
                         linkToBVI: Boolean
                       )

object ScoringModelApp extends App {
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  import spark.implicits._

  Logger.getRootLogger.setLevel(Level.WARN)
  val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

  // Read the CustomerDocument parquet file from Assessment 2
  val customerDocumentDS: Dataset[CustomerDocument] = spark.read.parquet(outputPath + "assessment2.parquet").as[CustomerDocument]
  customerDocumentDS.show(false)

  // Function to determine if a customer has a link to the British Virgin Islands
  def hasLinkToBVI(addresses: Seq[AddressData]): Boolean = {
    addresses.exists(_.country.contains("British Virgin Islands"))
  }

  // Transform the CustomerDocument dataset to ScoringModel dataset
  val scoringModelDS: Dataset[ScoringModel] = customerDocumentDS.map { customer =>
    ScoringModel(
      customer.customerId,
      customer.forename,
      customer.surname,
      Option(customer.accounts).getOrElse(Seq.empty),
      customer.address,
      linkToBVI = hasLinkToBVI(customer.address)
    )
  }

  scoringModelDS.show(false)

  // Count the number of customers with a link to the British Virgin Islands
  val countBVI = scoringModelDS.filter(_.linkToBVI).count()
  println(s"Number of customers with a link to the British Virgin Islands: $countBVI")
}
