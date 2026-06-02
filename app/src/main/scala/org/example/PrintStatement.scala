package org.example
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}


object PrintStatement {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("App")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n====================================")
    println(greeting())
    println("====================================\n")



    spark.stop()
  }

  def greeting(): String = "Hello, world!"
}
