import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Hive_Joins {

  def main(args:Array[String]): Unit ={

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")

    val sc=SparkSession
      .builder()
      .appName("Hive joins example")
      .master("local")
      .getOrCreate()

    sc.sparkContext.setLogLevel("ERROR")

    import sc.implicits._

    val empdata=sc.sparkContext.textFile("C:\\Users\\rv00451128\\IdeaProjects\\MyFirst\\emp.txt")
    val schemaString_emp="empid empname position salary mgrid deptid c7"
    val schema_emp = StructType(schemaString_emp.split(" ").map(fieldName1 ⇒ StructField(fieldName1, StringType, true)))
    val rowRDD = empdata.map(_.split(",")).map(e ⇒ Row(e(0), e(1),e(2),e(3),e(4),e(5),e(6)))
    val empdf=sc.createDataFrame(rowRDD,schema_emp)
    empdf.show()


    /*
    +----+-------+---------+----+----+---+---+
|  c1|     c2|       c3|  c4|  c5| c6| c7|
+----+-------+---------+----+----+---+---+
|1281|  Shawn|Architect|7890|1481| 10|IXZ|
|1381|  Jacob|    Admin|4560|1481| 20|POI|
|1481|  flink|      Mgr|9580|1681| 10|IXZ|
|1581|Richard|Developer|1000|1681| 40|LKJ|
|1681|   Mira|      Mgr|5098|1481| 10|IKZ|
|1781|   John|Developer|6500|1681| 10|IXZ|
+----+-------+---------+----+----+---+---+
    * */

    val dept=Seq(Row("10","INVENTORY","HYDERABAD","IXZ"),
    Row("20","ACCOUNTS","PUNE","POI"),
      Row("30","DEVELOPMENT","CHENNAI","LKJ")
    )

    val schemaString_dept="deptid deptname location c4"
    val schema_dept=StructType(schemaString_dept.split(" ").map(fieldname=>StructField(fieldname,StringType,true)))

    val deptdf=sc.createDataFrame(sc.sparkContext.parallelize(dept),schema_dept)
    deptdf.show()

  }

}
