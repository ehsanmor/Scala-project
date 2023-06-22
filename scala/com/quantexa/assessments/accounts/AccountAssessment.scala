package com.quantexa.assessments.accounts
import com._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




object AccountAssessment extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  //Create DataFrames of sources
  val customerDF: DataFrame = spark.read.option("header", "true")
    .csv("src/main/resources/customer_data.csv")
  val accountDF = spark.read.option("header", "true")
    .csv("src/main/resources/account_data.csv")

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

  //Create Datasets of sources
  val customerDS: Dataset[CustomerData] = customerDF.as[CustomerData]
  val accountDS: Dataset[AccountData] = accountDF.withColumn("balance", 'balance.cast("long")).as[AccountData]

  customerDS.show
  accountDS.show

  // Transforming and joining the data
  val groupedAccountData = accountDS
    .groupByKey(account => account.customerId)
    .mapGroups { case (customerId, accounts) =>
      val accountSeq = accounts.toSeq
      val totalBalance = accountSeq.map(_.balance).sum
      val averageBalance = totalBalance.toDouble / accountSeq.size
      (customerId, accountSeq, accountSeq.size, totalBalance, averageBalance)
    }

  val customerAccountOutputDS = customerDS.joinWith(groupedAccountData, customerDS("customerId") === groupedAccountData("_1"), "left_outer")
    .map { case (customer, groupedAccount) =>
      if (groupedAccount != null) {
        CustomerAccountOutput(
          customer.customerId,
          customer.forename,
          customer.surname,
          groupedAccount._2,
          groupedAccount._3,
          groupedAccount._4,
          groupedAccount._5
        )
      } else {
        CustomerAccountOutput(
          customer.customerId,
          customer.forename,
          customer.surname,
          Seq(),
          0,
          0,
          0.0
        )
      }
    }



  customerAccountOutputDS.show(1000, truncate = false)
  customerAccountOutputDS.write.mode("overwrite").parquet("src/main/resources/customerAccountOutputDS.parquet")


}