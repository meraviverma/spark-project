import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

// Hash Partitioner and Range Partitioner Example
object Class11 {

  def main(arg: Array[String]){
    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")
    val spark=SparkSession
      .builder()
      .appName("Class11")
      //.config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val textfile=spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\Employee.txt")

    println(textfile.collect().mkString(","));

    val sum=(((textfile.flatMap(a=>a.split(" "))).map(a=>(a,1)))).reduceByKey((x,y)=>(x+y))

    //saving output in a directory
    sum.saveAsTextFile("C:\\Users\\rv00451128\\Desktop\\tutorial\\data\\wordcount")

    // Hash Partitioner Example
    val counts = textfile.flatMap(line => line.split(" ")).map(word => (word, 1)).partitionBy(new HashPartitioner(10))

    counts.reduceByKey(_+_).saveAsTextFile("C:\\Users\\rv00451128\\Desktop\\tutorial\\data\\wordcount")

    println((((textfile.flatMap(a=>a.split(" "))).map(a=>(a,1)))).reduceByKey((x,y)=>(x+y)).collect().mkString("\n"))

    //Range Partitioned Example
    val counts_range = textfile.flatMap(line => line.split(" ")).map(word => (word, 1))
    val range=counts_range.partitionBy(new RangePartitioner(10,counts_range))

    //counts_range.reduceByKey(_+_).saveAsTextFile("C:\\Users\\rv00451128\\Desktop\\tutorial\\data\\range_ex")

    //Range Partitioned Example
    val data=Seq((1,11),(11,1111),(21,2121),(31,3131),(51,5151),(71,7171),(81,8181),(91,9191))
    val rdd=spark.sparkContext.parallelize(data)
    println(rdd.partitioner)
    println(rdd.partitions.size)

    val rangePartitioner=new RangePartitioner(4,rdd)
    val partrdd=rdd.partitionBy(rangePartitioner)
    println(partrdd.mapPartitionsWithIndex((i,iter)=>Iterator((i,iter.toMap))).collect().mkString(","))
    println(partrdd.partitioner)

    //more generalized way

    println((((textfile.flatMap(a=>a.split(" "))).map(a=>(a,1)))).reduceByKey((_+_)).collect().mkString("\n"))


    val aaa=spark.sparkContext.parallelize(Seq("a","b","c"))

  }
}
