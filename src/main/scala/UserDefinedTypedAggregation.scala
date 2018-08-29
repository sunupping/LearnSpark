import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * Created by jie.sun on 2018/8/6.
  */
/**
  * Created by jie.sun on 2018/8/6.
  */
object UserDefinedTypedAggregation {

  case class Employee(name:String ,salary:Long)
  case class Average(var sum:Long, var count:Long)

  object MyAverage extends Aggregator[Employee,Average,Double]{
    override def zero: Average = Average(0L, 0L)

    override def reduce(b: Average, a: Employee): Average = {
      b.sum += a.salary
      b.count += 1
      b
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("type-safe-UDAF").getOrCreate()
    import spark.implicits._
    val ds = spark.read
      .json("./resources/employees.json")
      .as[Employee]
    ds.show()
    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
//    result.write.csv("./udtadir")
    result.show()
    spark.stop()
  }
}
