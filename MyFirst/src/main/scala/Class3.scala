import org.apache.spark.sql.SparkSession

import scala.io.Source

//Broadcast variable and accumulator
object Class3 {
   def main(args: Array[String]){

     System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
     val spark=SparkSession
       .builder()
       .appName("Broadcast variable")
       .master("local")
       .getOrCreate()

     spark.sparkContext.setLogLevel("ERROR")

     //Scala way to import a file and convert to list
     //C:\Users\rv00451128\Desktop\tutorial\dataset\data-master\data-master\retail_db\products
val products=Source.fromFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\part-00000").getLines().toList


     products.foreach(println)

val productsMap=products.map(product =>(product.split(",")(0).toInt,product.split(",")(2))).toMap

     val orders=spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\orders-part-00000")
     orders.take(100).foreach(println)

     orders.map(order=>order.split(",")(3)).take(10).foreach(println)
     orders.map(order=>order.split(",")(3)).distinct.collect.foreach(println)

     //productsMap.foreach(println)
     //productsMap.get(1345) return Some(Nike Men's Home Game Jersey St. Louis Rams Gr) to get original type use get
     // Ex productsMap.get(1345).get
     println(productsMap.get(1345).get)

     //val bv = spark.sparkContext.broadcast(productsMap)
  }

}
