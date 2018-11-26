import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{expr,col,column,lit}

object Class16 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark=SparkSession
      .builder()
      .appName("join example")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._



    val myManualSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false))) // just to illustrate flipping ))

    val myrow=Seq(Row("Hello",null,1L))
    val myrdd=spark.sparkContext.parallelize(myrow)

    val mydf=spark.createDataFrame(myrdd,myManualSchema)

    mydf.show()

    val df = spark.read.format("json")
      .load("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\2015-summary.json")
    df.createOrReplaceTempView("dfTable")

    df.select("DEST_COUNTRY_NAME").show(2)

    df.select(df.col("DEST_COUNTRY_NAME"),col("DEST_COUNTRY_NAME"),column("DEST_COUNTRY_NAME"),
      'DEST_COUNTRY_NAME,$"DEST_COUNTRY_NAME",expr("DEST_COUNTRY_NAME"),expr("DEST_COUNTRY_NAME AS Destination")
      ,expr("DEST_COUNTRY_NAME as Destination").alias("DEST_COUNTRY")).show(2)

    df.selectExpr("DEST_COUNTRY_NAME as destcountry").show(2)

    df.selectExpr("*","(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME) as withinCountry").show(2)

    df.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME))").show()

    df.select(
      expr("*"),
      lit(1).as("something")
    ).show(2)

    df.withColumn("numberOne",lit(1)).show(2)

    df.withColumn("withincountry",expr("DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME")).show(2)

    df.withColumnRenamed("DEST_COUNTRY_NAME","dest").show(2)


    val dfwithlongcolumnname=df.withColumn("This long column name",expr("ORIGIN_COUNTRY_NAME"))

    dfwithlongcolumnname.selectExpr("`This long column name`","`This long column name` as `new col`").show(2)

    df.drop("count").show(2)

    df.printSchema()

    df.withColumn("count",col("count").cast("int")).printSchema()

    df.where(col("count")<2)
      .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
      .show(2)

    println(df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").count())

    println(df.select("ORIGIN_COUNTRY_NAME").distinct().count())

    val seed=5;
    val withReplacement=false
    val fraction=0.5

    println(df.sample(withReplacement,fraction,seed).count())

    df.sort("count").show(5)

    df.orderBy("count","DEST_COUNTRY_NAME").show(5)
  }
}
