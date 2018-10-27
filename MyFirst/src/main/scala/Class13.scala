import Case.Person
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._
object Class13 {
  case class Order(order_id: BigInt,order_date:String,order_customer_id:BigInt,order_status:String)
  def main(args:Array[String]): Unit ={
    val spark=SparkSession
      .builder()
      //.config("spark.sql.warehouse.dir", "file:///c:/Users/rv00451128/IdeaProjects/MyFirst/orders.txt")
      .appName("class13")
      .master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val orders=spark.read.textFile("c:/Users/rv00451128/IdeaProjects/MyFirst/orders.txt")
    println(orders.take(10).mkString("\n"))
    println(orders.map(o=>o.split(",")(1)).take(10).mkString("\n"))
    println(orders.printSchema())


    println("::Example of split and reduceByKey::")
    val od1=spark.sparkContext.textFile("c:/Users/rv00451128/IdeaProjects/MyFirst/orders.txt")

    od1.map(o=>(o.split(",")(3),1)).reduceByKey(_+_).collect.foreach(println)
    //group order by order status using RDD
   //val abc=orders.map(o=>(o.split(",")(3),1))




    val ordersjson=spark.read.json("c:/Users/rv00451128/IdeaProjects/MyFirst/orders_json")

    println(ordersjson.show())
    println(ordersjson.select("order_date").show())

    //resolved issue  Multiple sources found for csv (org.apache.spark.sql.execution.datasources.csv.CSVFileFormat,
    // com.databricks.spark.csv.DefaultSource15)

    val orderscsv=spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                  .option("header", "false")
                  .option("inferSchema", "true")
                  .load("c:/Users/rv00451128/IdeaProjects/MyFirst/orders_csv - Copy.csv")
                  .toDF("order_id","order_date","order_customer_id","order_status")
    println(orderscsv.printSchema())



    //using map to change datatype to int for orderid and customer id as default data type is string
    val orderdf: DataFrame =spark.read.textFile("c:/Users/rv00451128/IdeaProjects/MyFirst/orders.txt")
      .map(o=>{
        val a=o.split(",")
        (a(0).toInt,a(1),a(2).toInt,a(3))
      }).toDF("order_id","order_date","order_customer_id","order_status")

    println(orderdf.printSchema())

    val orderdf1 =spark.read.text("")
    val aaa: RDD[String] =spark.sparkContext.textFile("")


    /*Fix Error:(53, 25) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and
    Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in
    future releases.*/

    //Declare case class outside the object scope

   // case class Order(order_id: Int,order_date:String,order_customer_id:Int,order_status:String)
     val orderds: Dataset[Order] =spark.read.textFile("c:/Users/rv00451128/IdeaProjects/MyFirst/orders.txt")
      .map(o=>{
        val a=o.split(",")
        Order(a(0).toInt,a(1),a(2).toInt,a(3))
      }).as[Order]


    println(orderds.show())
    /*Case class person(name:String,age:Long)
    val css=Seq(Person("Andy",32)).toDS()*/

    val orderdsjson=spark.read.json("c:/Users/rv00451128/IdeaProjects/MyFirst/orders_json").as[Order]
    orderdsjson.show()
    //orderdsjson.write.avro("C:\\Users\\rv00451128\\Desktop\\tutorial\\data\\retail_db_avro\\order1")
    println(orderdsjson.groupBy("order_status").count().show())

    orderdsjson.createOrReplaceTempView("orders")
    spark.sql("select * from orders").show()
    val abc=spark.sql("select order_status,count(1) from orders group by order_status")

    //explain Physical execution plan
    abc.explain()

    //lower case a column in Dataframe
    //If you want to give alias then use .alias other wise it will show column name as lower(order_status)
    ordersjson.select($"order_id",lower($"order_status")).show()
    ordersjson.select(col("order_id"),lower(col("order_status")).alias("order_status")).show()

    //Get order year and month from the order date column
    ordersjson.select($"order_date",date_format($"order_date","YYYY-MM")).show()
    orderdsjson.withColumn("order_date",date_format($"order_date","YYYY")).show()
    //ordersjson.withColumn("order_id",lower(col("order_status"))).show
    /*import org.apache.spark.sql.hive.HiveContext
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext._*/

    val hiveContext=new HiveContext(spark.sparkContext)
    hiveContext.sql("select * from orders").show()

  }

}
