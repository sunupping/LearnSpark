import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by jie.sun on 2018/8/3.
  */
object UserDefinedUntypedAggregation {

  object MyAverage extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("inputcolumn",LongType)::Nil)

    override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(!input.isNullAt(0)){
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("udaf_avg").master("local").getOrCreate()
    spark.udf.register("avg1",MyAverage)

    val peopleDF = spark.read.json("D:\\ChromeDownloads\\spark-2.3.0-bin-hadoop2.6\\examples\\src\\main\\resources\\employees.json")
    peopleDF.createOrReplaceTempView("employees")
    import spark.sql
    sql("select * from employees").show()
    sql("select avg1(salary) as average_salary from employees").show()
    spark.stop()


  }
}
