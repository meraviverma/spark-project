import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]){
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    //val num=sc.parallelize(List("Ravi","is a good","boy"))
    val num=sc.parallelize(List(2,3,4,5))
    //val num=sc.textFile("D:/spark/sample.txt")

    //println("hello")
    val count=num.count()
    val result=num.map(word=>word+1)
    //val result1=num.map(map=>map.split(","))
    //println(result1.first().mkString(","))
    println(result.collect().mkString(","))
    //val c=maps.count()
    //println(c)
    println(count)
  }


}
