import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.udf.UDFType

@UDFType(stateful = true)
class AutoIncrementUDF extends UDF{

  var ctr=0;
  def evaluate(): Int ={

    ctr=ctr+1;
    return ctr;

  }

}
