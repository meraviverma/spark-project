import org.apache.spark.sql.SparkSession

import scala.io.Source

//Broadcast variable and accumulator
object Class3 {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("Broadcast variable")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //################## Example Accumulator #################################################

    val badpacket=spark.sparkContext.accumulator(0,"Bad packet")
    val zeroValueSales=spark.sparkContext.accumulator(0,"zero value sales")
    val missingFields=spark.sparkContext.accumulator(0,"missing fields")
    val blankline=spark.sparkContext.accumulator(0,"blanklines")
    val logdata=spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\purchaselog.txt")

    logdata.foreach{line=>
      if(line.length == 0) blankline += 1
      else if(line.contains("Bad data packet"))badpacket += 1
      else{
        val fields=line.split("\t")
        if (fields.length != 4) missingFields += 1
        else if (fields(3).toFloat == 0) zeroValueSales +=1
      }

    }
    println("Purchase Log Analysis Counters:")
    println(s"\tBad Data Packets=${badpacket.value}")
    println(s"\tZero Value Sales=${zeroValueSales.value}")
    println(s"\tMissing Fields=${missingFields.value}")
    println(s"\tBlank Lines=${blankline.value}")

    //OUTPUT
    /*Purchase Log Analysis Counters:
      Bad Data Packets=5
    Zero Value Sales=3
    Missing Fields=1
    Blank Lines=1*/

    //#########################################################################################

    //############################## Broadcast Variable Example ##############################


    //Scala way to import a file and convert to list
    //C:\Users\rv00451128\Desktop\tutorial\dataset\data-master\data-master\retail_db\products
    val products = Source.fromFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\part-00000").getLines().toList


    //products.foreach(println)

    val productsMap = products.map(product => (product.split(",")(0).toInt, product.split(",")(2))).toMap

    //C:\Users\rv00451128\Desktop\tutorial\dataset\data-master\data-master\retail_db\orders
    val orders = spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\orders-part-00000")
    //orders.take(100).foreach(println)

    /*orders.map(order => order.split(",")(3)).take(10).foreach(println)
    orders.map(order => order.split(",")(3)).distinct.collect.foreach(println)*/

    // var orderCompleteCount=0;
    val orderCompleteCount = spark.sparkContext.accumulator(0, "order com")
    val ordersFiltered = orders.
      filter(order => {
        orderCompleteCount += 1
        order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED"

      })

    val ordersMap=ordersFiltered.map(order => {
      (order.split(",")(0).toInt,order.split(",")(1))
    })

    val orderItems=spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\order_items")
    //val
    println(ordersFiltered.count())
    println(orderCompleteCount)

    //productsMap.foreach(println)
    //productsMap.get(1345) return Some(Nike Men's Home Game Jersey St. Louis Rams Gr) to get original type use get
    // Ex productsMap.get(1345).get
    //println(productsMap.get(1345).get)

    //val bv = spark.sparkContext.broadcast(productsMap)
  }

}
