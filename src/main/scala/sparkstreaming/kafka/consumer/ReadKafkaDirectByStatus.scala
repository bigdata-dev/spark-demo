package sparkstreaming.kafka.consumer

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by admin on 2016/10/31.
  */
object ReadKafkaDirectByStatus {

  def processRdd(rdd: RDD[(String, String)]) = {
    println("\n\nExecute process 'ProcessRdd'")
    try {
      //val rs: RDD[(String, Int)] = rdd.map(_._2).flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)
      val rs: RDD[(String)] = rdd.map(_._2)
      //val a = 3 / 0
      rs.collect.foreach(println)
    } catch {
      case e:Exception => println(e)
    }
  }

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
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))

    ssc.checkpoint("/tmp")

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

    val workCountDStream: DStream[(String, Int)] = kafkaStream.map(_._2).flatMap(_.split("\t")).map((_,1)).reduceByKey(_ + _)

    val statusRs: DStream[(String, Int)] = workCountDStream.updateStateByKey(updateFunc)

    statusRs.foreachRDD(rdd=>{
      println("\n\nPrint Result RDD")
      rdd.collect.foreach(println)
    })

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc = (value:Seq[Int],status:Option[Int])=>{
    val data: Int = value.foldLeft(0)(_+_)
    val last: Int = status.getOrElse(0)
    Some(data+last)
  }

}
