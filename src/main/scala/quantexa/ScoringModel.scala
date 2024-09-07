package quantexa

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/***
 * Part of the Quantexa solution is to flag high risk countries as a link to these countries may be an indication of
 * tax evasion.
 *
 * For this question you are required to populate the flag in the ScoringModel case class where the customer has an
 * address in the British Virgin Islands.
 *
 * This flag must be then used to return the number of customers in the dataset that have a link to a British Virgin
 * Islands address.
 */

object ScoringModel extends App {


  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[AccountData],
                               //Addresses for this customer
                               address: Seq[AddressData]
                             )

  case class ScoringModel(
                           customerId: String,
                           forename: String,
                           surname: String,
                           //Accounts for this customer
                           accounts: Seq[AccountData],
                           //Addresses for this customer
                           address: Seq[AddressData],
                           linkToBVI: Boolean
                         )


  //END GIVEN CODE

  val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

  val customerDocumentDS = spark.read.parquet(outputPath + "assessment2.parquet").as[CustomerDocument]

  val scoringModelDS = customerDocumentDS.map {
    customer =>
      ScoringModel(
        customerId = customer.customerId,
        forename = customer.forename,
        surname = customer.surname,
        accounts = customer.accounts,
        address = customer.address,
        linkToBVI = customer.address.map(_.country).contains(Option("British Virgin Islands"))
      )
  }

  val filteredDS = scoringModelDS.filter {
    score => score.linkToBVI
  }
  println(filteredDS.count)

}
