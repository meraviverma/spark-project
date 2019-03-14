import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark_Guide_Chapter6 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark=SparkSession
      .builder()
      .appName("Userdefinefunction")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val udfExampleDF=spark.range(5).toDF("num")
    def power3(number:Double):Double=number*number*number
   println( power3(2.0))
    val power3udf=udf(power3(_:Double):Double)

    udfExampleDF.select(power3udf(col("num"))).show()

    /* Output
    8.0
    +--------+
    |UDF(num)|
      +--------+
    |     0.0|
      |     1.0|
      |     8.0|
      |    27.0|
      |    64.0|
      +--------+ */

  }

}
