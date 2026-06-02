package org.example.scala_native_data_modelling
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}


import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}

/***
- Problem #3 Definition:


 * Part of the Quantexa solution is to flag high risk countries as a link to these countries may be an indication of
 * tax evasion.
 *
 * For this question you are required to populate the flag in the ScoringModel case class where the customer has an
 * address in the British Virgin Islands.
 *
 * This flag must be then used to return the number of customers in the dataset that have a link to a British Virgin
 * Islands address.
 */


/*

- We have the data models defined in previous exercises
- In scala projects, once we have compiled a certain class, that is available globally on the level of the project, as all classes are collected in the /build dir
- Thus we can use case classes for data modelling without redifing them.
*case classes do not have OOP attributes such as inheritance, thus we cannot simply inherit a previously defined data model and simply add a new column/attribute, we must define a new instance once more


case class AccountData(
    customerId: String,
    accountId:  String,
    balance:    Double
)

case class AddressDataFull(
    addressId:  String,
    customerId: String,
    address:    String,
    number:     Option[Int],
    road:       Option[String],
    city:       Option[String],
    country:    Option[String]
)

case class CustomerDocument(
    customerId: String,
    forename:   String,
    surname:    String,
    accounts:   Seq[AccountData],
    address:    Seq[AddressDataFull]
)

// scoring output case class
case class ScoringModelDocument(
    customerId:  String,
    forename:    String,
    surname:     String,
    accounts:    Seq[AccountData],
    address:     Seq[AddressDataFull],
    isHighRisk:  Boolean
)

*/

case class ScoringModel(
    customerId:  String,
    forename:    String,
    surname:     String,
    accounts:    Seq[Account],
    address:     Seq[AddressDataFull],
    isBVIRisk:  Boolean
)


object Ex3 extends App {

    val spark = SparkSession.builder()
        .appName("Ex3")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ==========================================================================
    // 1. import customer + address "customerAddressDocument" from parquet file
    // ==========================================================================
    val customerAddressDocumentDS: Dataset[CustomerDocument] = spark.read
        .option("header", "true")
        .parquet("src/main/resources/data/parquet/CustomerAddressDocument.parquet")
        .as[CustomerDocument]
    //customerAddressDocumentDS.show(15)
    //customerAddressDocumentDS.printSchema()


    val scoringModelDS: Dataset[ScoringModel] = customerAddressDocumentDS.map{
        row =>
            val bviBool = row.address.exists(_.country.contains("British Virgin Islands"))

            ScoringModel(
                customerId  = row.customerId,
                forename    = row.forename,
                surname     = row.surname,
                accounts    = row.accounts,
                address     = row.address,
                isBVIRisk   = bviBool
            )

    }.as[ScoringModel]
    //scoringModelDS.show(15)

    val bviRiskDS: Dataset[ScoringModel] = scoringModelDS.filter(_.isBVIRisk)
    // val bviRiskCount: Int = scoringModelDS.filter(_.isBVIRisk).count()
    // println(bviRiskCount)
    println(s"No. of accounts associated with the BVI is: ${bviRiskDS.count()}")
    println("Specific accounts associated with the BVI:")
    bviRiskDS.show(100)

    bviRiskDS.write.mode("overwrite").parquet("src/main/resources/data/parquet/BVI_Accounts.parquet")

    // print final of process
    println(s"Finished ${this.getClass.getSimpleName}")
    spark.stop()
}