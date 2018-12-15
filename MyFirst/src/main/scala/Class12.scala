import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._

object Class12 {

// Hive Windowing Function Example
  case class stock(date_ :String, Ticker:String, Open : Double, High : Double, Low : Double, Close : Double, Volume_for_the_day : Int)
  def main(args:Array[String]): Unit ={
    val spark=SparkSession
      .builder()
      .appName("Class12")
      .master("local").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val ordersjson=spark.read.json("c:/Users/rv00451128/IdeaProjects/MyFirst/student.json")

    ordersjson.createOrReplaceTempView("order")
    ordersjson.printSchema()

    //Explode function example
    val hivecontext=new HiveContext(spark.sparkContext)
    /*hivecontext.sql("select * from order").show()
    hivecontext.sql("select student,skill from order LATERAL VIEW explode(course) skill_set AS skill").show()
    hivecontext.sql("select count(*),skill  from (select student,skill from order LATERAL VIEW explode(course) skill_set AS skill) group by (skill)").show()
    hivecontext.sql("set spark.sql.caseSensitive=false")*/

    val stockdata =spark.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\spmohst.txt")
      .map(o=>{
        val a=o.split(",")
        (a(0),a(1),a(2).toDouble,a(3).toDouble,a(4).toDouble,a(5).toDouble,a(6).toInt)
      }).toDF("date_","ticker","open","high","low","close","volume_for_the_day")
    //val stk=stockdata.as[stock]
    stockdata.printSchema()

    stockdata.createOrReplaceTempView("stock");

    //Lag function
   // hivecontext.sql("select ticker,date_,close  from stock order by ticker").show()
    hivecontext.sql("select ticker,date_,close,lag(close,2) over( order by ticker) as yesterday_price from stock").show()

    //LEAD
    hivecontext.sql("select ticker,date_,close,lead(close,2) over( order by ticker) as yesterday_price from stock").show()
    hivecontext.sql("select ticker,date_,close,case(lead(close,1) over( order by ticker) - close )>0 when true then 'higher' when false then 'lesser' end as charges from stock").show()

    //FIRST_VALUE
    hivecontext.sql("select   ticker,first_value(high) over(partition by ticker order by ticker) as first_value from stock ").show()

    //LAST_VALUE
    hivecontext.sql("select   ticker,last_value(high) over(partition by ticker order by ticker) as last_value from stock ").show()
    //println(stockdata.take(3).mkString(","))

    //Count
    hivecontext.sql("Select ticker,count(ticker) over (partition by ticker order by ticker) as cnt from stock").show()

    //sum
    hivecontext.sql("select ticker,sum(close) over (partition by ticker order by date_ ) as total from stock").show()

    //min
    hivecontext.sql("select ticker,min(close) over (partition by ticker order by ticker) as minimum from stock").show()

    //max
    hivecontext.sql("select ticker,max(close) over (partition by ticker order by ticker) as maxmum from stock").show()

    //Average
    hivecontext.sql("select ticker,avg(close) over (partition by ticker order by ticker) as average from stock").show()

    //Rank
    hivecontext.sql("select ticker,close,rank() over (partition by ticker order by close) as closing from stock").show()

    //Cume_dist
    hivecontext.sql("select ticker,close,cume_dist() over (partition by ticker order by close) as cummulative from stock").show()

    //percent_rank()

    hivecontext.sql("select ticker,close,percent_rank() over (partition by ticker order by close) as percent_rank_closing from stock").show()

    hivecontext.sql("select ticker,close,Ntile() over (partition by ticker order by close) as Ntile_closing from stock").explain(true)

  }
}
