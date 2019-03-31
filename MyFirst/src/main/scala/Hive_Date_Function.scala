import org.apache.spark.sql.SparkSession

object Hive_Date_Function {

  def main(args:Array[String]):Unit= {

    System.setProperty("hadoop.home.dir", "D:\\software\\winutils-master\\hadoop")

    val sc = SparkSession
      .builder()
      .appName("Transformation and action example")
      .master("local")
      .getOrCreate()
    sc.sparkContext.setLogLevel("ERROR")

    /*FROM_UNIXTIME
    from_utc_timestamp
    to_utc_timestamp
    unix_timestamp
    unix_timestamp()
    unix_timestamp( string date )
    unix_timestamp( string date, string pattern )
    from_unixtime( bigint number_of_seconds  [, string format] )
    To_Date( string timestamp )
    YEAR( string date )
    MONTH( string date )
    DAY( string date ), DAYOFMONTH( date )
    HOUR( string date )
    MINUTE( string date )
    SECOND( string date )
    WEEKOFYEAR( string date )
    DATEDIFF( string date1, string date2 )
    DATE_ADD( string date, int days )
    DATE_SUB( string date, int days )
    DATE CONVERSIONS :
*/
    sc.sql("SELECT FROM_UNIXTIME(UNIX_TIMESTAMP()) as FROM_UNIXTIME ").show()

    sc.sql("SELECT  from_utc_timestamp('1970-01-01 07:00:00', 'JST')").show()

    sc.sql("SELECT to_utc_timestamp ('1970-01-01 00:00:00','America/Denver')").show()

    sc.sql("SELECT unix_timestamp ('2009-03-20', 'yyyy-MM-dd')").show()

    sc.sql("select UNIX_TIMESTAMP('2000-01-01 00:00:00')").show()

    sc.sql("select UNIX_TIMESTAMP('2000-01-01 10:20:30','yyyy-MM-dd')").show()

    sc.sql("select FROM_UNIXTIME( UNIX_TIMESTAMP() )").show()

    sc.sql("SELECT FROM_UNIXTIME(UNIX_TIMESTAMP())").show()

    sc.sql("select TO_DATE('2000-01-01 10:20:30')").show()

    sc.sql("select YEAR('2000-01-01 10:20:30')").show()

    sc.sql("select MONTH('2000-01-01 10:20:30')").show()

    sc.sql("SELECT DAY('2000-03-01 10:20:30')").show()

    sc.sql("SELECT HOUR('2000-03-01 10:20:30')").show()

    sc.sql("SELECT MINUTE('2000-03-01 10:20:30')").show()

    sc.sql("SELECT SECOND('2000-03-01 10:20:30')").show()

    sc.sql("SELECT WEEKOFYEAR('2000-03-01 10:20:30')").show()

    sc.sql("SELECT DATEDIFF('2000-03-01', '2000-01-10')").show()

    sc.sql("SELECT DATE_ADD('2000-03-01', 5)").show()

    sc.sql("SELECT DATE_SUB('2000-03-01', 5)").show()


    /*create table sample(rn int, dt string) row format delimited fields terminated by ',';
    select * from sample
      02111993
    03121994
    03131995
    04141996
    load data local inpath '/home/user/Desktop/sample.txt' into table sample;
    select cast(substring(from_unixtime(unix_timestamp(dt, 'MMddyyyy')),1,10) as date) from sample;
    OK
    1993-02-11
    1994-03-12
    1995-03-13
    1996-04-14
    Time taken: 0.112 seconds, Fetched: 4 row(s)*/

    /*
    +-------------------+
|      FROM_UNIXTIME|
+-------------------+
|2019-03-31 21:10:41|
+-------------------+

+---------------------------------------------------------------+
|from_utc_timestamp(CAST(1970-01-01 07:00:00 AS TIMESTAMP), JST)|
+---------------------------------------------------------------+
|                                            1970-01-01 16:00:00|
+---------------------------------------------------------------+

+------------------------------------------------------------------------+
|to_utc_timestamp(CAST(1970-01-01 00:00:00 AS TIMESTAMP), America/Denver)|
+------------------------------------------------------------------------+
|                                                     1970-01-01 07:00:00|
+------------------------------------------------------------------------+

+--------------------------------------+
|unix_timestamp(2009-03-20, yyyy-MM-dd)|
+--------------------------------------+
|                            1237487400|
+--------------------------------------+

+--------------------------------------------------------+
|unix_timestamp(2000-01-01 00:00:00, yyyy-MM-dd HH:mm:ss)|
+--------------------------------------------------------+
|                                               946665000|
+--------------------------------------------------------+

+-----------------------------------------------+
|unix_timestamp(2000-01-01 10:20:30, yyyy-MM-dd)|
+-----------------------------------------------+
|                                      946665000|
+-----------------------------------------------+

+--------------------------------------------------------------------------------------------+
|from_unixtime(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss), yyyy-MM-dd HH:mm:ss)|
+--------------------------------------------------------------------------------------------+
|                                                                         2019-03-31 21:10:44|
+--------------------------------------------------------------------------------------------+

+--------------------------------------------------------------------------------------------+
|from_unixtime(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss), yyyy-MM-dd HH:mm:ss)|
+--------------------------------------------------------------------------------------------+
|                                                                         2019-03-31 21:10:45|
+--------------------------------------------------------------------------------------------+

+------------------------------+
|to_date('2000-01-01 10:20:30')|
+------------------------------+
|                    2000-01-01|
+------------------------------+

+---------------------------------------+
|year(CAST(2000-01-01 10:20:30 AS DATE))|
+---------------------------------------+
|                                   2000|
+---------------------------------------+

+----------------------------------------+
|month(CAST(2000-01-01 10:20:30 AS DATE))|
+----------------------------------------+
|                                       1|
+----------------------------------------+

+---------------------------------------------+
|dayofmonth(CAST(2000-03-01 10:20:30 AS DATE))|
+---------------------------------------------+
|                                            1|
+---------------------------------------------+

+--------------------------------------------+
|hour(CAST(2000-03-01 10:20:30 AS TIMESTAMP))|
+--------------------------------------------+
|                                          10|
+--------------------------------------------+

+----------------------------------------------+
|minute(CAST(2000-03-01 10:20:30 AS TIMESTAMP))|
+----------------------------------------------+
|                                            20|
+----------------------------------------------+

+----------------------------------------------+
|second(CAST(2000-03-01 10:20:30 AS TIMESTAMP))|
+----------------------------------------------+
|                                            30|
+----------------------------------------------+

+---------------------------------------------+
|weekofyear(CAST(2000-03-01 10:20:30 AS DATE))|
+---------------------------------------------+
|                                            9|
+---------------------------------------------+

+------------------------------------------------------------+
|datediff(CAST(2000-03-01 AS DATE), CAST(2000-01-10 AS DATE))|
+------------------------------------------------------------+
|                                                          51|
+------------------------------------------------------------+

+-------------------------------------+
|date_add(CAST(2000-03-01 AS DATE), 5)|
+-------------------------------------+
|                           2000-03-06|
+-------------------------------------+

+-------------------------------------+
|date_sub(CAST(2000-03-01 AS DATE), 5)|
+-------------------------------------+
|                           2000-02-25|
+-------------------------------------+

    * */


  }

}
