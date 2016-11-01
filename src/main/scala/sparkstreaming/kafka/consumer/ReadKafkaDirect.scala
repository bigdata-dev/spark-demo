package sparkstreaming.kafka.consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by admin on 2016/10/31.
  */
object ReadKafkaDirect {
  def main(args: Array[String]) {
    if(args.length<2){
      println("Usage:ReadKafka ryxc163:2181,ryxc164:2181,ryxc165:2181 consumeMsgDataTimeInterval(secs)")
      System.exit(1)
    }

    val Array(zkServers,processingInterval) = args;
    val conf = new SparkConf().setAppName("ReadKafka")
    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))

    //设置检查点目录
    ssc.checkpoint("/tmp")

    val kafkaStream = KafkaUtils.createStream(
      ssc,
      zkServers,
      "kafkaReadGroup1",
      Map("user-behavior-topic" -> 1)
    )



    kafkaStream.foreachRDD(rdd => {
      val collect: Array[(String, String)] = rdd.collect
      val x= rdd.count
      println("\n\nNumber of records in this batch : " +x)
      if (x > 0) {// RDD has data
        for(line <- collect.toArray){
          println("###########:"+line._2)
          /*  var index = line._2.split("\t").view(0).toString          // That is the index
          var timestamp = line._2.split("\t").view(1).toString   // This is the timestamp from source
          var security =  line._2.split("\t").view(1).toString   // This is the name of the security
          var price = line._2.split(',').view(3).toFloat        // This is the price of the security*/
        }
      }
    })

    val rs: DStream[(String, Int)] = kafkaStream.map(_._2).flatMap(_.split("\t")).map((_,1)).reduceByKey(_ + _)


    val statusrs: DStream[(String, Int)] = rs.updateStateByKey(updateFunc)

    //打印结果集
    statusrs.print
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc = (value:Seq[Int],status:Option[Int])=>{
    val data: Int = value.foldLeft(0)(_+_)
    val last: Int = status.getOrElse(0)
    Some(data+last)
  }

}
