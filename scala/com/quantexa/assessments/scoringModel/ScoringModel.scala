package com.quantexa.assessments.scoringModel

import com.quantexa.assessments.accounts.AccountAssessment.AccountData
import com.quantexa.assessments.customerAddresses.CustomerAddress.AddressData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession,DataFrame, Dataset}



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

  // Load  data
  val customerDocument: Dataset[CustomerDocument] = spark
    .read
    .parquet("src/main/resources/customerDocument.parquet")
    .as[CustomerDocument]


  // Populate linkToBVI and map to ScoringModel
  val scoringModels: Dataset[ScoringModel] = customerDocument.map(doc => {
    val linkToBVI = doc.address.exists(_.country == Option("British Virgin Islands"))
    ScoringModel(
      doc.customerId,
      doc.forename,
      doc.surname,
      doc.accounts,
      doc.address,
      linkToBVI
    )
  })


  // Count customers with a link to BVI
  val countLinkedToBVI: Long = scoringModels.filter(_.linkToBVI).count()

  println(s"There are $countLinkedToBVI customers linked to the British Virgin Islands.")



}
