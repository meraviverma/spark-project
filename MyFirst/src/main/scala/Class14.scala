import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Class14 {

  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("Class11")
      //.config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    //spark.sparkContext.textFile gives RDD
    val orders=spark.sparkContext.textFile("c:/Users/rv00451128/IdeaProjects/MyFirst/orders.txt")


  //FlatMap Example
    val x = spark.sparkContext.parallelize(Array(1,2,3))
    val y = x.flatMap(n => Array(n, n*100, 42))
    println(x.collect().mkString(", "))
    println(y.collect().mkString(", "))

    println(y.toDebugString)

    //Map Example
    val xmap = spark.sparkContext.parallelize(Array("b", "a", "c"))
    val ymap = xmap.map(z => (z,1))
    println(xmap.collect().mkString(", "))
    println(ymap.collect().mkString(", "))

    //Group By
    val xg = spark.sparkContext.parallelize(
      Array("John", "Fred", "Anna", "James"))
    val yg = xg.groupBy(w => w.charAt(0))
    println(yg.collect().mkString(", "))

    //GROUP BY KEY
    val xgk = spark.sparkContext.parallelize(
      Array(('B',5),('B',4),('A',3),('A',2),('A',1)))
    val ygk = xgk.groupByKey()
    println(xgk.collect().mkString(", "))
    println(ygk.collect().mkString(", "))

   /* val l=List("raVi   ","   vermA    ")
    val ab=l.map(x=>x.trim().toUpperCase.substring(1,x.length).toLowerCase)
*/
    /*val l=List("raVi   ","   vermA    ")
    val ab=l.map(x=>x.trim().toUpperCase.toLowerCase())
*/
  }
}
