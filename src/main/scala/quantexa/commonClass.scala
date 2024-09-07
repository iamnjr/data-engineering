//package quantexa
//
//object commonClass {
//  case class CustomerData(
//                           customerId: String,
//                           forename: String,
//                           surname: String
//                         )
//
//  case class AccountData(
//                          customerId: String,
//                          accountId: String,
//                          balance: Long
//                        )
//
//  case class CustomerAccountOutput(
//                                    customerId: String,
//                                    forename: String,
//                                    surname: String,
//                                    accounts: Seq[AccountData],
//                                    numberAccounts: Int,
//                                    totalBalance: Long,
//                                    averageBalance: Double
//                                  )
//  case class AddressRawData(
//                             addressId: String,
//                             customerId: String,
//                             address: String
//                           )
//
//  case class AddressData(
//                          addressId: String,
//                          customerId: String,
//                          address: String,
//                          number: Option[Int],
//                          road: Option[String],
//                          city: Option[String],
//                          country: Option[String]
//                        )
//
//  case class CustomerDocument(
//                               customerId: String,
//                               forename: String,
//                               surname: String,
//                               accounts: Seq[AccountData],
//                               address: Seq[AddressData]
//                             )
//
//  case class ScoringModel(
//                           customerId: String,
//                           forename: String,
//                           surname: String,
//                           accounts: Seq[AccountData],
//                           address: Seq[AddressData],
//                           linkToBVI: Boolean
//                         )
//
//}
