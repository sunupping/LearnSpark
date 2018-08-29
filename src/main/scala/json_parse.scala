import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by jie.sun on 2018/8/20.
  */
object json_parse {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("explode_json")
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    import spark.implicits._
    val jsonPath = args(0)
    val df = spark.read.json(jsonPath)
    //df.show()
    //df.printSchema()
    val dfScore = df.select(df("name"),df("age"),df("myScore.math"),df("myScore.yuwen"),df("myScore.english"),df("time"))
      .toDF("name","age","math","yuwen","english","time")
    dfScore.createOrReplaceTempView("tmp_json")
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val time = args(0).split("/")(5)
//    dfScore.write.partitionBy("time").format("hive").saveAsTable("test1.explode")  //库表已经存在，不能是已经存在的目录
    sql(s"INSERT OVERWRITE TABLE test1.explodee  partition (datetime='$time') select name,age,math,yuwen,english from tmp_json" )
    spark.stop()
  }
}

