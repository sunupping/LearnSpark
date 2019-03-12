import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/**
  * Created by jie.sun on 2018/10/25.
  */
object BiKafkaStreaming {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  groupid
                            |  checkpointDir
                            |  batchDuration
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics,groupid,checkpointDir,batchDuration) = args
//    val brokers = "data03:9092,data04:9092,data05:9092"
//    val topics = "bitest"
//    val groupid = "jiegroup01"
//    val checkpointDir = "D:\\scala\\bikafka"
//    val batchDuration = 2

    // Create context with 2 second batch interval
    //set("spark.io.compression.codec","snappy") ,因为默认是laz4，这里没有
    val sparkConf = new SparkConf().setAppName("DirectKafkaBiCount").set("spark.io.compression.codec","snappy").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration.toInt))
    //使用 updateStateByKey 方法必须要指定 checkpoint目录
    ssc.checkpoint(checkpointDir)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",  //latest
      "enable.auto.commit" -> (false: java.lang.Boolean)  //为了自己管理offect ,必须false
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)

    def addFunc = (currentValues :Seq[Long], prevValueState: Option[Long]) => {
        val currentCount = currentValues.sum
        val previousCount = prevValueState.getOrElse(0L)  //之前的变量若非空即返回，否则返回后面括号里计算后的值，即0L
        Some(currentCount + previousCount)
      }
    //从非json 字符串中抽取数据  lines.map(x => (x.split(",")(2).split(":")(2) , 1L)).reduceByKey(_+_)
    //全量统计
    val allCounts = lines.map(x => (x.split("\"")(6), 1L )).updateStateByKey[Long](addFunc)
    //按照shopId统计，并top排序
    val shopIdCounts =lines.map(x => (x.split(",")(2).split(":")(2) , 1L)).updateStateByKey[Long](addFunc).transform{
      rdd => rdd.sortBy({case(w,c) => c}, false)
    }
//   报错关于序列化
//    val shopIdCounts = lines.map{x => if(x.count() < 1) return ;(x.substring(84,lines.toString.length-3) , 1L)}.reduceByKey(_+_).transform(rdd => rdd.sortBy({case(w,c) => c}, false))
    //结果输出
    allCounts.print()
    shopIdCounts.print(2)


//    val words = lines.flatMap(_.split(" "))
//    val wordCounts = word.map(x => (x, 1L)).reduceByKey(_ + _)
//    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
