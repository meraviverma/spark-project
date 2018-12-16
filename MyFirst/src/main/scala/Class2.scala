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

    //OUTPUT
    //(two,2),(one,1),(three,3)

    // Example group By Key

    val pairrdd1: RDD[(String, Int)] =spark.sparkContext.parallelize(words).map(words => (words,1))
    val wordcount1=pairrdd1.groupByKey().collect()
    val summed=wordcount1.map{case (str,nums)=>(str,nums.sum)} // This will help to sum the compact buffer.
    println(wordcount1.mkString(","))
    println(summed.mkString(","))

    //OUTPUT
    //(two,CompactBuffer(1, 1)),(one,CompactBuffer(1)),(three,CompactBuffer(1, 1, 1))
    //(two,2),(one,1),(three,3)

    println("############################# Combine By Key Example 1 ###############################")

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

    //OUTPUT
   /* 2
    2
    4
    ScoreDetail(B,Math,75.0)
    ScoreDetail(B,English,78.0)
    ScoreDetail(C,Math,90.0)
    ScoreDetail(C,English,80.0)
    ScoreDetail(A,Math,98.0)
    ScoreDetail(A,English,88.0)
    ScoreDetail(D,Math,91.0)
    ScoreDetail(D,English,80.0)*/

    // Combine the scores for each student
    val avgScoresRDD = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1) /*createCombiner*/,
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1) /*mergeValue*/,
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) /*mergeCombiners*/
      // calculate the average
    ).map( { case(key, value) => (key, value._1/value._2) })

    avgScoresRDD.collect.foreach(println)

    //OUTPUT
    /*(B,76.5)
    (C,85.0)
    (A,93.0)
    (D,85.5)*/

    println("############################# Combine By Key Example 2 ###############################")
    //Combine By Key Example

    // Creating PairRDD studentRDD with key value pairs
    val studentRDD = spark.sparkContext.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
      ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
      ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
      ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
      ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
      ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
      ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
      ("Juan", "Biology", 60)), 3)

    //Defining createCombiner, mergeValue and mergeCombiner functions
    def createCombiner = (tuple: (String, Int)) =>
      (tuple._2.toDouble, 1)

    def mergeValue = (accumulator: (Double, Int), element: (String, Int)) =>
      (accumulator._1 + element._2, accumulator._2 + 1)

    def mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)


    // use combineByKey for finding percentage
    val combRDD = studentRDD.map(t => (t._1, (t._2, t._3)))
      .combineByKey(createCombiner, mergeValue, mergeCombiner)
      .map(e => (e._1, e._2._1/e._2._2))

    //Check the Outout
    combRDD.collect foreach println

    // Output
    // (Tina,76.5)
    // (Thomas,86.25)
    // (Jackeline,76.5)
    // (Joseph,82.5)
    // (Juan,64.0)
    // (Jimmy,77.0)
    // (Cory,65.0)

    println("############################# Aggreagte By Key Example ###############################")
    //  aggregateByKey example in scala
    // Creating PairRDD studentRDD with key value pairs
    val studentRDD1 = spark.sparkContext.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
      ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
      ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
      ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
      ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)

    //Defining Seqencial Operation and Combiner Operations
    //Sequence operation : Finding Maximum Marks from a single partition
    def seqOp = (accumulator: Int, element: (String, Int)) =>
      if(accumulator > element._2) accumulator else element._2

    //Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
    def combOp = (accumulator1: Int, accumulator2: Int) =>
      if(accumulator1 > accumulator2) accumulator1 else accumulator2

    //Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
    val zeroVal = 0
    val aggrRDD1 = studentRDD1.map(t => (t._1, (t._2, t._3)))
      .aggregateByKey(zeroVal)(seqOp, combOp)

    //Check the Outout
    aggrRDD1.collect foreach println

    // Output
    // (Tina,87)
    // (Thomas,93)
    // (Jackeline,83)
    // (Joseph,91)
    // (Juan,69)
    // (Jimmy,97)
    // (Cory,71)

    ///////////////////////////////////////////////////////
    // Let's Print Subject name along with Maximum Marks //
    ///////////////////////////////////////////////////////

    println("##################### Subject name along with Maximum Marks ##########################")
    //Defining Seqencial Operation and Combiner Operations
    def seqOp1 = (accumulator: (String, Int), element: (String, Int)) =>
      if(accumulator._2 > element._2) accumulator else element

    def combOp1 = (accumulator1: (String, Int), accumulator2: (String, Int)) =>
      if(accumulator1._2 > accumulator2._2) accumulator1 else accumulator2

    //Zero Value: Zero value in our case will be tuple with blank subject name and 0
    val zeroVal1 = ("", 0)

    val aggrRDD2 = studentRDD1.map(t => (t._1, (t._2, t._3)))
      .aggregateByKey(zeroVal1)(seqOp1, combOp1)
    //Check the Outout
    aggrRDD2.collect foreach println

    // Check the Output
    // (Tina,(Biology,87))
    // (Thomas,(Physics,93))
    // (Jackeline,(Biology,83))
    // (Joseph,(Chemistry,91))
    // (Juan,(Physics,69))
    // (Jimmy,(Chemistry,97))
    // (Cory,(Chemistry,71))

    println("###################### printing over all percentage of all students using aggregateByKey #########")
    //Defining Seqencial Operation and Combiner Operations
    def seqOp3 = (accumulator: (Int, Int), element: (String, Int)) =>
      (accumulator._1 + element._2, accumulator._2 + 1)

    def combOp3 = (accumulator1: (Int, Int), accumulator2: (Int, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)

    //Zero Value: Zero value in our case will be tuple with blank subject name and 0
    val zeroVal3 = (0, 0)

    // Here aggregateByKey() will return seperate counts of total marks and total subject
    // So we need to convert it in a percentage value by using a separate map() function
    val aggrRDD3 = studentRDD.map(t => (t._1, (t._2, t._3)))
      .aggregateByKey(zeroVal3)(seqOp3, combOp3)
      .map(t => (t._1, t._2._1/t._2._2*1.0))

    //Check the Outout
    aggrRDD3.collect foreach println

    // Output
    // (Tina,76.0)
    // (Thomas,86.0)
    // (Jackeline,76.0)
    // (Joseph,82.0)
    // (Juan,64.0)
    // (Jimmy,77.0)
    // (Cory,65.0)

    println("############################# Aggreagte By Key Example 2 ###############################")

    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
    val data = spark.sparkContext.parallelize(keysWithValuesList)
    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    countByKey.collect foreach println

    //OUTPUT
    /*(bar,3)
    (foo,5)*/

    println("############################# Fold ###############################")

    val foldrdd = spark.sparkContext.parallelize(1 to 5)
    val result=foldrdd.fold(0){(acc,element)=>acc+element}
    println(result) //output = 15
    val resultfold=spark.sparkContext.parallelize(1 to 5).fold(0){(acc2,element2)=> acc2 + 1}
    println(resultfold)



    println("############################# Fold By Key ###############################")

    val deptEmployees = List(
      ("cs",("jack",1000.0)),
      ("cs",("bron",1200.0)),
      ("phy",("sam",2200.0)),
      ("phy",("ronaldo",500.0))
    )
    val employeeRDD = spark.sparkContext.makeRDD(deptEmployees)

    val maxByDept = employeeRDD.foldByKey(("dummy",0.0))((acc,element)=> if(acc._2 > element._2) acc else element)

    println("maximum salaries in each dept" + maxByDept.collect().toList)

    //OUTPUT
   // maximum salaries in each deptList((phy,(sam,2200.0)), (cs,(bron,1200.0)))

  }

}
