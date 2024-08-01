package quantexa

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

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

//case class CustomerDocument(
//                             customerId: String,
//                             forename: String,
//                             surname: String,
//                             accounts: Seq[AccountData],
//                             address: Seq[AddressData]
//                           )

object customerAddress extends App {
  def addressParser(unparsedAddress: AddressRawData): AddressData = {
    val split = unparsedAddress.address.split(", ")
    AddressData(
      addressId = unparsedAddress.addressId,
      customerId = unparsedAddress.customerId,
      address = unparsedAddress.address,
      number = split.headOption.flatMap(s => if (s.forall(_.isDigit)) Some(s.toInt) else None),
      road = split.lift(1),
      city = split.lift(2),
      country = split.lift(3)
    )
  }

  val conf = new SparkConf().setAppName("quantexaAssessment2").setMaster("local[*]").set("spark.driver.host", "localhost")

  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  val inputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/SparkTestProject"
  val outputPath = "file:///Users/niranjankashyap/Downloads/SparkTestProjectWithoutAnswers/"

  // Load the parquet file from Assessment 1
  val customerAccountDS = spark.read.parquet(outputPath + "assessment1.parquet").as[CustomerAccountOutput]
  customerAccountDS.show(false)

  // Load address data
  val addressDS = spark.read.option("header", "true").csv(inputPath + "/src/main/resources/address_data.csv").as[AddressRawData]
  addressDS.show(false)

  // Parse the address data
  val parsedAddressDS: Dataset[AddressData] = addressDS.map(addressParser)
  parsedAddressDS.show(false)

  // Join customerAccountDS with parsedAddressDS
  val joinDS = customerAccountDS.joinWith(parsedAddressDS, customerAccountDS("customerId") === parsedAddressDS("customerId"), "left_outer")
    .groupByKey { case (customerAccount, _) => customerAccount.customerId }
    .mapGroups { case (customerId, iter) =>
      val grouped = iter.toList
      val customerAccount = grouped.head._1
      val addresses = grouped.map(_._2)

      // Handle None for accounts
      val accounts = if (customerAccount.accounts == null) Seq.empty[AccountData] else customerAccount.accounts

      CustomerDocument(customerAccount.customerId, customerAccount.forename, customerAccount.surname, accounts, addresses)
    }

  joinDS.show(false)
  joinDS.printSchema()

  // Save the final dataset
  joinDS.write.mode("overwrite").parquet(outputPath + "assessment2.parquet")
}
