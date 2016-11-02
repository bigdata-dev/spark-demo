package sparkstreaming.kafka.consumer

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Created by tangfei on 2016/10/28.
  */
object WebPagePopularityValueCalculator {
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println( s"""
                             |Usage: DirectKafkaWordCount <brokers> <topics> <groupid> <processingInterval>
                             |  <brokers> is a list of one or more Kafka brokers
                             |  <topics> is a list of one or more kafka topics to consume from
                             |  <groupid> is a consume group
                             |  <processingInterval> is  execution time interval
        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers, topics, groupId,processingInterval) = args

    // Create context with (processingInterval) second batch interval
    val conf = new SparkConf().setAppName("ReadKafkaDirect")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))
    //using updateStateByKey asks for enabling checkpoint
    ssc.checkpoint(checkpointDir)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)
    val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    kafkaStream.foreachRDD(rdd => {
      println("\n\nNumber of records in this batch : " +rdd.count())
      rdd.collect.map(_._2).foreach(println)
      if (!rdd.isEmpty()) {
        // 先处理消息
        processRdd(rdd)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })

    val msgDataRDD = kafkaStream.map(_._2)
    //for debug use only
    //println("Coming data in this interval...")
    msgDataRDD.print()

  }


  def processRdd(rdd: RDD[(String, String)]) = {
  }

}
