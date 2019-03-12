/**
  * Created by jie.sun on 2018/9/13.
  */
/**
  * Created by jie.sun on 2018/9/13.
  */
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

object SparkStreamKafka {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

//    val Array(brokers, topics) = args
    val conf = new SparkConf().setAppName("SparkStreamKafka")
    val streamingContext = new StreamingContext(conf,Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topicA", "topicB")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.topic(),record.partition(),record.offset(),record.key, record.value))

    // Import dependencies and create kafka params as in Create Direct Stream above
    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

//    val rdd = KafkaUtils.createRDD[String,String](sparkContext, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent)

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    // The details depend on your data store, but the general idea looks like this

    // begin from the the offsets committed to the database
//    val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
//      new TopicPartition(resultSet.string("topic"), resultSet.int("partition")) -> resultSet.long("offset")
//    }.toMap

//    val stream = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
//    )

//    stream.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

//      val results = yourCalculation(rdd)

      // begin your transaction

      // update results
      // update offsets where the end of existing offsets matches the beginning of this batch of offsets
      // assert that offsets were updated correctly

      // end your transaction
//    }
  }
}
