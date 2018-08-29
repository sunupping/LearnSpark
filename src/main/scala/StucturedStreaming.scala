import org.apache.spark.sql.SparkSession

/**
  * Created by jie.sun on 2018/8/3.
  */
object StucturedStreaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StructuredNetworkWordCount")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    //sqlDataFrame
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = lines.writeStream
      .outputMode("Append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
