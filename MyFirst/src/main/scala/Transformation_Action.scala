import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.tools.nsc.transform.patmat.Lit

object Transformation_Action {
  def main(args:Array[String]):Unit={

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")

    val sc=SparkSession
      .builder()
      .appName("Transformation and action example")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")



    println("######## MAP - TRANSFORMATION ###########")
    //1) MAP - TRANSFORMATION
    val data: RDD[String] =sc.sparkContext.parallelize(List("a","b","c"))
    val datamap=data.map(a=>(a,1))
    println(datamap.collect().mkString(","))

    //------------OUTPUT---------
    //(a,1),(b,1),(c,1)

    //2)Filter - TRANSFORMATION

    println("######## FILTER - TRANSFORMATION ###########")
    val x = sc.sparkContext.parallelize(List(1,2,3,4,5))
    val yfd=x.filter(_>3)
    println(yfd.collect().mkString(","))

    /*------------OUTPUT---------
    4,5*/


    //3)FLATMAP - TRANSFORMATION
    //RETURN A NEW RDD by first applying a function to all elements of this RDD, and then flattening the results

    println("######## FLATMAP - TRANSFORMATION ###########")
    val xfmap=sc.sparkContext.parallelize(List(1,2,3))
    val yfmap=xfmap.flatMap(n=>Array(n,n*100,42))

    println(yfmap.collect().mkString(","))

    println("######## GROUP BY  - TRANSFORMATION ###########")
    val xgb =sc.sparkContext.parallelize(List("John","Fred","Anna","James"))
    val ygb=xgb.groupBy(x=>x.charAt(0))
    println(ygb.collect().mkString(","))

    val newdf=sc.sparkContext.parallelize(List("1","2","3")).toDF()


    newdf.withColumn("abc",monotonically_increasing_id()).show()
    newdf.withColumn("def",lit(1)).show()

    newdf.withColumnRenamed("value","literal").show()

    newdf.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)


    newdf.na.drop()
    newdf.na.drop("any")
    newdf.na.drop("all", Seq("StockCode", "InvoiceNo"))

    newdf.na.fill("All Null values become this string")
    newdf.na.fill(5, Seq("StockCode", "InvoiceNo"))
    val fillColValues = Map(
      "StockCode" -> 5,
      "Description" -> "No Value"
    )
    newdf.na.fill(fillColValues)






  }

}
