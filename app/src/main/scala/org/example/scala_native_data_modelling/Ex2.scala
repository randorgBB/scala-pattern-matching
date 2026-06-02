package org.example.scala_native_data_modelling
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}


// data import classes
case class CustomerAccountSnap(
    customerId: String,
    forename:   String,
    surname:    String,
    accounts:   Seq[Account]
)

case class AddressRawData(
    addressId:  String,
    customerId: String,
    address:    String
)

// intermediary case classes
case class AddressDataFull(
    addressId:  String,
    customerId: String,
    address:    String,
    number:     Option[Int],
    road:       Option[String],
    city:       Option[String],
    country:    Option[String],
)

// final output case classes
case class CustomerDocument(
    customerId: String,
    forename: String,
    surname: String,
    accounts: Seq[Account],
    address:  Seq[AddressDataFull]
)


//
object Ex2 extends App {

  val spark = SparkSession.builder()
      .appName("Ex2")
      .master("local[*]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // =======================================================
  // 1. Read customer snap parquet data
  // =======================================================
  val customerDataDS: Dataset[CustomerAccountSnap] = spark.read
      .option("header", "true")
      .parquet("src/main/resources/data/parquet/AccountCustomerOutput.parquet")
      .select(
          "customerId",
          "forename",   
          "surname",
          "accounts"
      )
      .as[CustomerAccountSnap]
  //customerDataDS.show(15)

  // =======================================================
  // 2. Read raw address data
  // =======================================================
  val addressRawDS: Dataset[AddressRawData] = spark.read
      .option("header", "true")
      .csv("src/main/resources/data/csv/address_data.csv")
      .as[AddressRawData]
  //addressRawDS.show(15)

  // =======================================================
  // 3. create new map -> Address DS to have another col with split addresses
  // =======================================================
  val addressMapDS: Dataset[AddressDataFull] = addressRawDS.map { 
      row =>
          val addressSeq: Array[String] = row.address.split(", ")
          AddressDataFull(
              // 1. address raw data
              addressId  = row.addressId,
              customerId = row.customerId,
              address    = row.address,
              
              // 2. map address string to 4 different attributes
              number  = addressSeq.lift(0).flatMap(s => scala.util.Try(s.toInt).toOption),
              road    = addressSeq.lift(1),
              city    = addressSeq.lift(2),
              country = addressSeq.lift(3)
          )
  }
  //addressMapDS.show(15)

  // =======================================================
  // 4. join Customer-Account Snap with Address Map
  // =======================================================
  val customerAddressDocumnentDS: Dataset[CustomerDocument] = customerDataDS
      .joinWith(addressMapDS, customerDataDS("customerId") === addressMapDS("customerId"))
      .map { 
          case (cust, addr) =>
              CustomerDocument(
                  customerId = cust.customerId,
                  forename   = cust.forename,
                  surname    = cust.surname,
                  accounts   = cust.accounts,
                  address    = Seq(addr)
              )
      }
      .as[CustomerDocument]
  customerAddressDocumnentDS.show(15)

  // =======================================================
  // 5. write customer - address map to parquet file
  // =======================================================
  customerAddressDocumnentDS.write.mode("overwrite").parquet("src/main/resources/data/parquet/CustomerAddressDocument.parquet")


  // print final of process
  println(s"Finished processing ${this.getClass.getSimpleName}")
  spark.stop() 

}

