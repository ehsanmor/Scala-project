package com.quantexa.assessments.customerAddresses

import com._
import com.quantexa.assessments.accounts.AccountAssessment.{AccountData, CustomerAccountOutput, customerDS}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}






object CustomerAddress extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("CustomerAddress").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

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

  //Expected Output Format
  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[AccountData],
                               //Addresses for this customer
                               address: Seq[AddressData]
                             )




  def addressParser(unparsedAddress: Seq[AddressData]): Seq[AddressData] = {
    unparsedAddress.map(address => {
      val split = address.address.split(", ")

      address.copy(
        number = Some(split(0).toInt),
        road = Some(split(1)),
        city = Some(split(2)),
        country = Some(split(3))
      )
    }
    )
  }

  val addressDF: DataFrame = spark.read.option("header", "true").csv("src/main/resources/address_data.csv")
  val addressData: Dataset[AddressRawData] = addressDF.as[AddressRawData] //convert to dataset of type AddressRawData
  addressData.show

  val customerData = spark.read.parquet("src/main/resources/customerAccountOutputDS.parquet").as[CustomerAccountOutput]
  customerData.show


  val groupedAddresses: Dataset[AddressData]= addressData.map(
    address=>AddressData(
      address.addressId,
      address.customerId,
      address.address,
      None,
      None,
      None,
      None
    )
    ).groupByKey(_.customerId).flatMapGroups(
      (customerId, addresses) => addressParser(addresses.toSeq)
    )

  groupedAddresses.show




  val customerDocument: Dataset[CustomerDocument] =  groupedAddresses
    .joinWith(customerData,groupedAddresses("customerId")===customerData("customerId"))
    .map {
      case (groupedAddress, customer) => {
        CustomerDocument(
          customer.customerId,
          customer.forename,
          customer.surname,
          customer.accounts,
          Seq(groupedAddress)
        )
      }
    }

  customerDocument.show(1000, truncate = false)
  customerDocument.write.mode("overwrite").parquet("src/main/resources/customerDocument.parquet")





}