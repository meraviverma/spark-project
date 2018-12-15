import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Class1 {
  case class orderdetail(order_customer_id:BigInt,order_date:String,order_id:BigInt,order_status:String)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("join example")
      .master("local")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //Rdd
    val rdd=spark.sparkContext.parallelize(List(1,2,3));
    println(rdd.collect().mkString(","));
    println(rdd.partitions.size)

    //Dataset
    val ds: Dataset[String] =spark.read.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\Employee.txt")
    ds.show();

    val rdd1: RDD[String] =spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\Employee.txt")
    println(rdd1.collect().mkString(","));

    val df: RDD[Row] =spark.read.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\Employee.txt").toDF().rdd
   //df.show()

    val orderds =spark.read.json("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\orders_json").as[orderdetail].toDF()

    orderds.show()

    //orderds.filter(_.order_status=="COMPLETE").filter(_.order_id==5).show()



  }

}
