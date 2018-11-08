import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    spark.sqlContext.sql("select * from person ").show()
    spark.sqlContext.sql("select * from graduateProgram ").show()
    spark.sqlContext.sql("select * from sparkStatus ").show()

    val joinExpression=person.col("graduate_program") === graduateProgram.col("id")
    var jointypeinner="inner"
    var jointypeouter="outer"
    var jointypeleftouter="left_outer"
    var jointyperightouter="right_outer"
    var jointypesemi="left_semi"
    var jointypeleftanti="left_anti"
    var jointypecrossjoin="cross"

    person.join(graduateProgram,joinExpression).show()
    person.join(graduateProgram,joinExpression,jointypeinner).show()
    person.join(graduateProgram,joinExpression,jointypeouter).show()

    person.join(graduateProgram,joinExpression,jointypeleftouter).show()
    graduateProgram.join(person,joinExpression,jointypeleftouter).show()

    person.join(graduateProgram,joinExpression,jointyperightouter).show()

    person.join(graduateProgram,joinExpression,jointypesemi).show()
    graduateProgram.join(person,joinExpression,jointypesemi).show()

    person.join(graduateProgram,joinExpression,jointypeleftanti).show()
    graduateProgram.join(person,joinExpression,jointypeleftanti).show()

    person.join(graduateProgram,joinExpression,jointypecrossjoin).show()
    graduateProgram.join(person,joinExpression,jointypecrossjoin).show()


    person.withColumnRenamed("id","personId")
      .join(sparkStatus,expr("array_contains(spark_status,id)"))
      .show()

    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")

    val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
    person.join(gradProgramDupe, joinExpr).show()

    //will get error, Exception in thread "main"
    // org.apache.spark.sql.AnalysisException: Reference 'graduate_program' is ambiguous, could be: graduate_program, graduate_program.;
    //person.join(gradProgramDupe, joinExpr).select("graduate_program").show()

    person
      .join(gradProgramDupe, "graduate_program")
      .select("graduate_program")
      .show()

    person
      .join(gradProgramDupe,joinExpr)
      .drop(person.col("graduate_program"))
      .select("graduate_program")
      .show()

    person
      .join(gradProgramDupe,joinExpr)
      .drop(person.col("graduate_program"))
      .show()

    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    val joinExpr2 = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr2).show()

    graduateProgram.join(person,joinExpression,jointypecrossjoin).explain()
  }
}
