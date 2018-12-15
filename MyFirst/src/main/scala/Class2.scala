import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Class2 {
  case class ScoreDetail(studentname:String,subject:String,score:Float)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark = SparkSession
      .builder()
      .appName("join example")
      .master("local")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Example Reduce By Key
    val words=Array("one","two","two","three","three","three")
    val pairrdd: RDD[(String, Int)] =spark.sparkContext.parallelize(words).map(words => (words,1))
    val wordcount=pairrdd.reduceByKey(_+_).collect()
    println(wordcount.mkString(","))

    // Example group By Key
    //val words=Array("one","two","two","three","three","three")
    val pairrdd1: RDD[(String, Int)] =spark.sparkContext.parallelize(words).map(words => (words,1))
    val wordcount1=pairrdd1.groupByKey().collect()
    val summed=wordcount1.map{case (str,nums)=>(str,nums.sum)} // This will help to sum the compact buffer.
    println(wordcount1.mkString(","))
    println(summed.mkString(","))

    //Combine By Key
    val scores = List(ScoreDetail("A", "Math", 98), ScoreDetail("A", "English", 88),
      ScoreDetail("B", "Math", 75), ScoreDetail("B", "English", 78),
      ScoreDetail("C", "Math", 90), ScoreDetail("C", "English", 80),
      ScoreDetail("D", "Math", 91), ScoreDetail("D", "English", 80))

    val scoreWithKey=for{i <- scores}yield (i.studentname,i)
    val scoresWithKeyRDD=spark.sparkContext.parallelize(scoreWithKey)
      .partitionBy(new HashPartitioner(3))
      .cache()

    scoresWithKeyRDD.foreachPartition(partition => println(partition.length))
    scoresWithKeyRDD.foreachPartition(partition =>(partition.foreach(item => println(item._2))))

    // Combine the scores for each student
    val avgScoresRDD = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1) /*createCombiner*/,
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1) /*mergeValue*/,
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) /*mergeCombiners*/
      // calculate the average
    ).map( { case(key, value) => (key, value._1/value._2) })

    avgScoresRDD.collect.foreach(println)

  }

}
