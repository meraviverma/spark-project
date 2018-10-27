//package com.bnsf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.IsNull


object Case {
  case class Person(name: String, age: Int)
  def main(args: Array[String]) {

    import org.apache.spark.sql.SparkSession


    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/Users/rv00451128/IdeaProjects/MyFirst/Employee.txt")
      .appName("data set")
      .master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val rdd_person = spark.read.textFile("C:/Users/rv00451128/IdeaProjects/MyFirst/Employee.txt")

    val rdd_person_schema = rdd_person.map(row => row.split(',')).map(field => Person(field(0), field(1).trim().toInt))

    val personDF = rdd_person_schema.toDF()
    //add new column with null value
    val newDf = personDF.withColumn("id", when($"age" === "26" or $"age" === "25", 2).otherwise(null))
    //filter null row
    //newDf.collect()
println(newDf.collect().mkString(","))
    //val newdf2 = newDf.filter($"id".isNotNull)
  val newdf2=newDf.drop("id","age")
    //val newdf2=newDf.re
    newdf2.createTempView("Employee")
    val selectDF = spark.sql("SELECT * FROM Employee")
    selectDF.show()

    spark.newSession()

  }
}