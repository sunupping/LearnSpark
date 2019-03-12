import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
  * Created by jie.sun on 2018/8/16.
  */
/**
  * Created by jie.sun on 2018/8/16.
  */
object explode_json {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("explode_json").master("local").getOrCreate()
//    val jsonPath = " file:///explode_json/people_Array.json"
    val jsonPath = "D:\\scala\\people_Array.json"
    val df = spark.read.json(jsonPath)
    df.show()
    df.printSchema()
    val dfScore = df.select(df("name"),explode(df("myScore"))).toDF("name","myScore")
    println("dfScore.show()-----------------")
    dfScore.show()
    val dfMyScore = dfScore.select("name","myScore.score1","myScore.score2")
    println("dfMyScore.show()-----------------")
    dfMyScore.show()
    dfMyScore.createOrReplaceTempView("explode")
    val result = spark.sql(s"select name,score1,score2 from explode")
    result.show()
  }
}
