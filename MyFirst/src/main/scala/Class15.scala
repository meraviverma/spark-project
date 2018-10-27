import org.apache.spark.sql.SparkSession

//Join Example
object Class15 {

  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark=SparkSession
      .builder()
      .appName("join example")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    /*person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    spark.sqlContext.sql("select * from person ").show()*/

    val joinExpression=person.col("graduate_program") === graduateProgram.col("id")
    var jointypeinner="inner"
    var jointypeouter="outer"
    var jointypeleftouter="left_outer"
    var jointyperightouter="right_outer"
    var jointypesemi="left_semi"
    var jointypeleftanti="left_anti"
    var jointypecross="cross"

    person.join(graduateProgram,joinExpression).show()
    person.join(graduateProgram,joinExpression,jointypeinner).show()
    person.join(graduateProgram,joinExpression,jointypeouter).show()
    person.join(graduateProgram,joinExpression,jointypeleftouter).show()
    person.join(graduateProgram,joinExpression,jointyperightouter).show()
    person.join(graduateProgram,joinExpression,jointypesemi).show()
    person.join(graduateProgram,joinExpression,jointypeleftanti).show()
    person.join(graduateProgram,joinExpression,jointypecross).show()

    //join

    //println(joinExpression)
  }
}
