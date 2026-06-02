package org.example.scala_native_data_modelling
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//import org.apache.spark.sql.functions._

// import data/csv case classes
case class Account(
    customerId: String,
    accountId:  String,
    balance:    Double
)

case class Customer(
    customerId: String,
    forename:   String,
    surname:    String
)

// output case class
case class AccountCustomerOutput(
    customerId:     String,
    forename:       String,
    surname:        String,
    accounts:       Seq[Account],
    numberAccounts: Int,
    totalBalance:   Double,
    averageBalance: Double
)

// beginning of the application
object Ex1 extends App {

    val spark = SparkSession.builder()
        .appName("Ex1")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ============================================================
    // *** Data Ingestion
    // Read CSV files into Datasets with case class mapping
    // ============================================================
    val customersDS: Dataset[Customer] = spark.read
        .option("header", "true")
        .csv("src/main/resources/data/csv/customer_data.csv")
        .as[Customer]

    val accountsDS: Dataset[Account] = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("src/main/resources/data/csv/account_data.csv")
        .as[Account]


    // ============================================================
    // *** Preferred Approach: 4
    // joinWith -> groupByKey -> mapGroups (pure typed)
    // ============================================================
    val outputDS: Dataset[AccountCustomerOutput] = customersDS
        .joinWith(
            accountsDS,
            customersDS("customerId") === accountsDS("customerId")
        )
        .groupByKey{case (customer, _) => customer.customerId}
        .mapGroups { 
          case (customerId, pairs) =>
            val records      = pairs.toSeq
            val customer     = records.map{
                case (customer, _) => customer
            }.head
            val accounts     = records.map{
                case (_, account) => 
                  Account(account.customerId, account.accountId, account.balance)
            }.toSeq
            val totalBalance = accounts.map(_.balance).sum // or accounts.map(_.balance).foldLeft(0.0)(_ + _)

            AccountCustomerOutput(
                customerId     = customerId,
                forename       = customer.forename,
                surname        = customer.surname,
                accounts       = accounts,
                numberAccounts = accounts.size,
                totalBalance   = totalBalance,
                averageBalance = totalBalance / accounts.size
            )
        }.as[AccountCustomerOutput]

    outputDS.printSchema()
    outputDS.show()
    // Dataset write-> Parquet
    outputDS.write.mode("overwrite").parquet("src/main/resources/data/parquet/AccountCustomerOutput.parquet")

    // ============================================================
    // *** Alternative Approaches: 1, 2, 3, 5
    // - Approach 1: Join -> GroupBy -> Agg (SQL/DataFrame style)
    // - Approach 2: Join -> as[CaseClass] -> groupByKey -> mapGroups
    // - Approach 3: groupByKey -> mapGroups -> joinWith (SQL/DataFrame style with groupByKey)
    // - Approach 5: joinWith -> groupByKey -> mapGroups
    //               readable vanilla scala, no SQL functions

    /*/ ============================================================
    // Approach 1: Join -> GroupBy -> Agg (SQL/DataFrame style)
    // ============================================================
    val result1 = customersDS
        .join(accountsDS, "customerId")
        .groupBy("customerId", "forename", "surname")
        .agg(
            collect_list(
                struct("customerId", "accountId", "balance")
            ).as("accounts"),
            count("*").cast("int").as("numberAccounts"),
            sum("balance").as("totalBalance"),
            avg("balance").as("averageBalance")
        )
        .as[AccountCustomerOutput3]

    println("=== Approach 1: Join -> GroupBy -> Agg ===")
    result1.printSchema()
    result1.show(truncate = false)

    // ============================================================
    // Approach 2: Join -> as[CaseClass] -> groupByKey -> mapGroups
    // ============================================================
    val result2 = customersDS
        .join(accountsDS, "customerId")
        .as[JoinedData3]
        .groupByKey(_.customerId)
        .mapGroups { case (customerId, rows) =>
            val records        = rows.toSeq
            val accounts       = records.map(r =>
                AccountData3(r.customerId, r.accountId, r.balance)
            )
            val totalBalance   = accounts.map(_.balance).sum
            AccountCustomerOutput3(
                customerId     = customerId,
                forename       = records.head.forename,
                surname        = records.head.surname,
                accounts       = accounts,
                numberAccounts = accounts.size,
                totalBalance   = totalBalance,
                averageBalance = totalBalance / accounts.size
            )
        }

    println("=== Approach 2: Join -> as[] -> groupByKey -> mapGroups ===")
    result2.printSchema()
    result2.show(truncate = false)

    // ============================================================
    // Approach 3: groupByKey -> mapGroups -> joinWith (SQL/DataFrame style with groupByKey)
    // ============================================================
    val accountsGrouped = accountsDS
        .groupByKey(_.customerId)
        .mapGroups { case (customerId, accounts) =>
            (customerId, accounts.toSeq)
        }

    val result3 = customersDS
        .joinWith(
            accountsGrouped,
            customersDS("customerId") === accountsGrouped("_1"),
            "left"
        )
        .map { 
            case (customer, accountsGrouped) =>
                val accounts     = Option(accountsGrouped)
                                    .map(_._2)
                                    .getOrElse(Seq.empty)
                val totalBalance = accounts.map(_.balance).sum
                AccountCustomerOutput3(
                    customerId     = customer.customerId,
                    forename       = customer.forename,
                    surname        = customer.surname,
                    accounts       = accounts,
                    numberAccounts = accounts.size,
                    totalBalance   = totalBalance,
                    averageBalance = if (accounts.nonEmpty)
                                        totalBalance / accounts.size
                                    else 0.0
                )
        }

    println("=== Approach 3: groupByKey -> mapGroups -> joinWith ===")
    result3.printSchema()
    result3.show(truncate = false)

    // ============================================================
    // Approach 4: joinWith -> groupByKey -> mapGroups (pure typed)
    // ============================================================
    val result4: Dataset[AccountCustomerOutput] = customersDS
        .joinWith(
            accountsDS,
            customersDS("customerId") === accountsDS("customerId")
        )
        .groupByKey { 
          case (customer, _) => customer.customerId 
        }
        .mapGroups { 
          case (customerId, pairs) =>
            val records      = pairs.toSeq
            val customer     = records.head._1
            val accounts     = records.map(_._2)
            val totalBalance = accounts.map(_.balance).sum // or accounts.map(_.balance).foldLeft(0.0)(_ + _)

            AccountCustomerOutput(
                customerId     = customerId,
                forename       = customer.forename,
                surname        = customer.surname,
                accounts       = accounts,
                numberAccounts = accounts.size,
                totalBalance   = totalBalance,
                averageBalance = totalBalance / accounts.size
            )
        }.as[AccountCustomerOutput]

    println("=== Approach 4: joinWith -> groupByKey -> mapGroups ===")
    result4.printSchema()
    result4.show() //truncate = false)

    // ============================================================
    // Approach 5: joinWith -> groupByKey -> mapGroups
    //             readable vanilla scala, no SQL functions
    //             explicit types, named helpers, clear steps
    // ============================================================
    val result5 = customersDS
        .joinWith(
            accountsDS,
            customersDS("customerId") === accountsDS("customerId")
        )
        .groupByKey { case (customer, _) =>
            customer.customerId
        }
        .mapGroups { case (customerId, pairs) =>

            // 0. materialize iterator
            val records: Seq[(Customer, Account)] = pairs.toSeq

            // 1. extract customer info from first record
            val customer: Customer = records.head._1

            // 2. extract all accounts as clean Seq
            val accounts: Seq[Account] = records.map {
                case (_, account) => account
            }

            // 3. compute aggregates with named steps
            val balances:       Seq[Double] = accounts.map(_.balance)
            val totalBalance:   Double      = balances.sum
            val numberAccounts: Int         = accounts.size
            val averageBalance: Double      = totalBalance / numberAccounts

            // 4. build output
            AccountCustomerOutput(
                customerId     = customerId,
                forename       = customer.forename,
                surname        = customer.surname,
                accounts       = accounts,
                numberAccounts = numberAccounts,
                totalBalance   = totalBalance,
                averageBalance = averageBalance
            )
        }

    println("=== Approach 5: Readable Vanilla Scala ===")
    result5.printSchema()
    result5.show() //truncate = false) */


    println(s"Finished processing ${this.getClass.getSimpleName}")
    spark.stop()
}