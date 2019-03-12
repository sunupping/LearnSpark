
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jie.sun on 2018/10/31.
  */
object DirectKafkaExample {

//  def setupSsc(): StreamingContext ={
//
//    def main(args: Array[String]) {
//
//      val ssc =  setupSsc
//      ssc.start()
//      ssc.awaitTermination()
//
//
//    }
//    val conf = new SparkConf().setAppName("CustomDirectKafkaExample").setMaster("local")
//    val kafkaParams:Map[String,String] = Map("metadata.broker.list" -> "slave1:9092,slave2:9092,slave3:9092")
//    val topicsSet = Set("testha")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val messages = createCustomDirectKafkaStream(ssc,kafkaParams,"master0:2181,slave1:2181,slave3:2181","/mysefloffset", topicsSet).map(_._2)
//
//    messages.foreachRDD{rdd => {
//      rdd.foreachPartition { partitionOfRecords =>
//        if(partitionOfRecords.isEmpty)
//        {
//          println("此分区数据为空.")
//        }
//        else
//        {
//          partitionOfRecords.foreach(println(_))
//        }
//      }
//
//    }
//    }
//    ssc
//  }


//  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String
//                                    , zkPath: String, topics: Set[String]): InputDStream[(String, String)] = {
//    val topic = topics.last
//    val zkClient = new ZkClient(zkHosts, 30000, 30000)
//    //val storedOffsets = readOffsets(zkClient,zkHosts, zkPath, topic)
//
//topic//    val kafkaStream = storedOffsets match {
////      case None => //最新的offset
////        KafkaUtils.createDirectStream[String, String](
////          ssc,
////          LocationStrategies.PreferConsistent,
////          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
////
////      case Some(fromOffsets) => // offset从上次继续开始
////        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
////        KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, fromOffsets, messageHandler)
////        KafkaUtils.createDirectStream[String, String](
////          ssc,
////          LocationStrategies.PreferConsistent,
////          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,fromOffsets.map((x,y) => x)))
// //   }
//
//    // save the offsets
////    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient,zkHosts, zkPath, rdd))
////    kafkaStream
//
//  }



//  private def readOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, topic: String):Option[Map[TopicAndPartition, Long]] = {
//
//    println("开始读取从zk中读取offset")
//
//    val stopwatch = new Stopwatch()
//
//    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
//    offsetsRangesStrOpt match {
//      case Some(offsetsRangesStr) =>
//        println(s"读取到的offset范围: ${offsetsRangesStr}")
//        val offsets = offsetsRangesStr.split(",")
//          .map(s => s.split(":"))
//          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
//          .toMap
//        println("读取offset结束: " + stopwatch)
//        Some(offsets)
//      case None =>
//        println("读取offset结束: " + stopwatch)
//        None
//    }
//  }

//  private def saveOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, rdd: RDD[_]): Unit = {
//    println("开始保存offset到zk中去")
//
//    val stopwatch = new Stopwatch()
//    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//    //分区,offset
//    offsetsRanges.foreach(offsetRange => println(s"Using ${offsetRange}"))
//
//    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}").mkString(",")
//    println("保存的偏移量范围:"+ offsetsRangesStr)
//    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
//    println("保存结束,耗时 ：" + stopwatch)
//  }

//  class Stopwatch {
//    private val start = System.currentTimeMillis()
//    override def toString() = (System.currentTimeMillis() - start) + " ms"
//  }
}
