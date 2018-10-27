import org.apache.spark.sql.SparkSession

object Dataframe {
  def main(args: Array[String]){

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/Users/rv00451128/IdeaProjects/MyFirst")
      .appName("data frame")
      .master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
    //val rdd_person = spark.read.textFile("C:/Users/rv00451128/IdeaProjects/MyFirst/Employee.txt")
    val rdd_person = spark.read.textFile("C:/Users/rv00451128/IdeaProjects/MyFirst/spmohst.txt")
      //.toDF("date","ticker","open","high","low","close","Volume_for_the_day")
    println(rdd_person.printSchema())
    //val rdd_person = spark.read.textFile("D:/spark/sample.txt")
    //val abc=spark.sparkContext.parallelize(List(1,2,3,4))

    //println(rdd_person.collect().mkString(","))
  }
}
