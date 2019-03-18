import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Transformation_Action {
  def main(args:Array[String]):Unit={

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")

    val sc=SparkSession
      .builder()
      .appName("Transformation and action example")
      .master("local")
      .getOrCreate()
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
    val xgb=sc.sparkContext.parallelize(List("John","Fred","Anna","James"))
    val ygb=xgb.groupBy(x=>x.charAt(0))
    println(ygb.collect().mkString(","))



  }

}
