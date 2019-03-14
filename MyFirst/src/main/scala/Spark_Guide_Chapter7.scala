import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object Spark_Guide_Chapter7 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("Userdefinefunction")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\online-retail-dataset.csv")
      .coalesce(5)

    df.cache()
    df.createOrReplaceTempView("dfTable")

    //count
    df.select(count("StockCode")).show()

    //countDistinct
    df.select(countDistinct("StockCode")).show()

    //approx_count_distinct
    df.select(approx_count_distinct("StockCode")).show()
    df.select(approx_count_distinct("StockCode",0.1)).show()

    //first and last
    df.select(first("StockCode"),last("StockCode")).show()

    //min and max
    df.select(min("Quantity"),max("Quantity")).show()

    //sum
    df.select(sum("Quantity")).show()

    //sumDistinct
    df.select(sumDistinct("Quantity")).show()

    //avg
    df.select(
      count("Quantity").alias("total_tran"),
      sum("Quantity").alias("total_pur"),
      avg("Quantity").alias("avg_pur"),
      expr("mean(Quantity)").alias("mean_purchases"))
        .selectExpr("total_pur/total_tran","avg_pur","mean_purchases").show()


    //groupBy
    df.groupBy("InvoiceNo").agg("Quantity"->"avg","Quantity"->"stddev_pop").show()


    //Window Function
    val dfWithDate=df.withColumn("date",to_date(col("InvoiceDate"),"MM/dd/yyyy HH:mm"))


    dfWithDate.createOrReplaceTempView("dfWithDate")

    val windowSpec = Window
      .partitionBy("CustomerId","date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity").over(windowSpec))

    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    /*dfWithDate.where("CustomerID IS NOT NULL").orderBy("CustomerID").select(
        col("CustomerID"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(5)*/





  }

}
