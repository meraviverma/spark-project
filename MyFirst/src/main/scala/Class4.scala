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
