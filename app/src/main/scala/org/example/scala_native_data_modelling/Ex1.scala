package org.example.scala_native_data_modelling
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}


object Ex1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex1")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n====================================")
    println(greeting())
    println("====================================\n")



    spark.stop()
  }

  def greeting(): String = "Exercise 1: Scala Native Data Modelling"
}
