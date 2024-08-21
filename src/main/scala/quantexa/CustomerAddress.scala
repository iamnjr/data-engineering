package quantexa

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

//case class CustomerAccountOutput(
//                                  customerId: String,
//                                  forename: String,
//                                  surname: String,
//                                  accounts: Option[Seq[AccountData]],
//                                  numberAccounts: Option[Int],
//                                  totalBalance: Option[Long],
//                                  averageBalance: Option[Double]
//                                )

case class AddressRawData(
                           addressId: String,
                           customerId: String,
                           address: String
                         )

case class AddressData(
                        addressId: String,
                        customerId: String,
                        address: String,
                        number: Option[Int],
                        road: Option[String],
                        city: Option[String],
                        country: Option[String]
                      )

case class CustomerDocument(
                             customerId: String,
                             forename: String,
                             surname: String,
                             accounts: Option[Seq[AccountData]],
                             numberAccounts: Option[Int],
                             totalBalance: Option[Long],
                             averageBalance: Option[Double],
                             address: Seq[AddressData]
                           )

object CustomerAddress {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("quantexaAssessment2").setMaster("local[*]").set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.WARN)

    val inputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/SparkTestProject"
    val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

    // Load data from CSV files into Dataset
    val customerAccountOutputDS = spark.read.parquet(outputPath + "assessment1.parquet").as[CustomerAccountOutput]
    val addressRawDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/address_data.csv").as[AddressRawData]

    customerAccountOutputDS.show(false)
    addressRawDS.show(false)

    // Define the address parser
    def addressParser(unparsedAddress: Seq[AddressRawData]): Seq[AddressData] = {
      unparsedAddress.map { address =>
        val parts = address.address.split(", ").toList

        val number = parts.headOption.flatMap(p => scala.util.Try(p.toInt).toOption)
        val road = parts.lift(1)
        val city = parts.lift(2)
        val country = parts.lift(3)

        AddressData(
          address.addressId,
          address.customerId,
          address.address,
          number,
          road,
          city,
          country
        )
      }
    }

    // Parse addresses and join with customer data
    val parsedAddressDS = addressRawDS.groupByKey(_.customerId)
      .mapGroups { case (customerId, addresses) =>
        (customerId, addressParser(addresses.toSeq))
      }.toDF("customerId", "address")

    val customerDocumentDS: Dataset[CustomerDocument] = customerAccountOutputDS
      .join(parsedAddressDS, Seq("customerId"), "left_outer")
      .as[(String, String, String, Option[Seq[AccountData]], Option[Int], Option[Long], Option[Double], Seq[AddressData])]
      .map {
        case (customerId, forename, surname, accounts, numberAccounts, totalBalance, averageBalance, address) =>
          CustomerDocument(
            customerId,
            forename,
            surname,
            accounts,
            numberAccounts,
            totalBalance,
            averageBalance,
            address
          )
      }

    customerDocumentDS.show(false)
    customerDocumentDS.printSchema()

    customerDocumentDS.write.mode("overwrite").parquet(outputPath + "assessment2.parquet")

  }
}
