import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Class4 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    /*val sparkconf=new SparkConf().setAppName("").setMaster("")
    val sc=new SparkContext(sparkconf)*/

    val spark = SparkSession
      .builder()
      .appName("Class4")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val data: DataFrame =spark.read.option("header","true").csv("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\Dept.csv")
    data.show()

    //data.toDebugString()

    val myrdd: RDD[Long] =spark.sparkContext.range(1,10,2).filter(e=>e>5)
    println(myrdd.collect().mkString(","))
    myrdd.filter(e=>e>5).foreach(println)
    println(myrdd.toDebugString)
    //val rdd1=spark.sparkContext.parallelize(myrdd)

    val emptyrdd=spark.sparkContext.parallelize(Seq())
    emptyrdd.foreach(println)

    val erdd=spark.sparkContext.emptyRDD

    //spark.sparkContext.textFile("")
    //spark.sparkContext.wholeTextFiles("",4)

    val abc=Seq(1,2,3)
    val abcrdd=spark.sparkContext.makeRDD(abc)
    abcrdd.collect().foreach(println)

    val rdd1 =  spark.sparkContext.parallelize(
                      List("yellow", "red", "blue","cyan", "black"), 3)

    val mapped =   rdd1.mapPartitionsWithIndex{
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //                         in the partition
      (index, iterator) => {
        println("Called in Partition -> " + index)
        val myList = iterator.toList
        // In a normal user case, we will do the
        // the initialization(ex : initializing database)
        // before iterating through each element
        myList.map(x => x + " -> " + index).iterator
      }
    }
    println(mapped.collect().mkString(","))



    /*output---------
          +----+---+
      | _c0|_c1|
      +----+---+
      |Name| id|
      |ravi| 20|
      +----+---+
     */

    //val data = spark.read.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\Dept.txt")
    // print(data.collect())

  }

}
