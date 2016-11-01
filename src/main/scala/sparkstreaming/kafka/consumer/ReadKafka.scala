package sparkstreaming.kafka.consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by admin on 2016/10/31.
  */
object ReadKafka {
  def main(args: Array[String]) {
    if(args.length<2){
      println("Usage:WebPagePopularityValueCalculator ryxc163:2181,ryxc164:2181,ryxc165:2181 consumeMsgDataTimeInterval(secs)")
      System.exit(1)
    }

    val Array(zkServers,processingInterval) = args;
    val conf = new SparkConf().setAppName("WebPagePopularityValueCalculator")
    conf.setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))

    val kafkaStream = KafkaUtils.createStream(
      ssc,
      zkServers,
      "kafkaReadGroup1",
      Map("user-behavior-topic" -> 1)
    )

    kafkaStream.foreachRDD()
    val lines = kafkaStream.map(_._2)
    lines.print

    lines.foreachRDD()


    ssc.start()
    ssc.awaitTermination()
  }

}
