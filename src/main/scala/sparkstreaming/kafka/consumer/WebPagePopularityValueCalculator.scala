package sparkstreaming.kafka.consumer

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Created by tangfei on 2016/10/28.
  */
object WebPagePopularityValueCalculator {
  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
  def main(args: Array[String]) {
    if(args.length<2){
      println("Usage:WebPagePopularityValueCalculator ryxc163:2181,ryxc164:2181,ryxc165:2181 consumeMsgDataTimeInterval(secs)")
      System.exit(1)
    }

    val Array(zkServers,processingInterval) = args;
    val conf = new SparkConf().setAppName("WebPagePopularityValueCalculator")
    conf.setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))

    //using updateStateByKey asks for enabling checkpoint
    ssc.checkpoint(checkpointDir)

    val kafkaStream = KafkaUtils.createStream(
      ssc,
      zkServers,
      msgConsumerGroup,
      Map("user-behavior-topic" -> 3)
    )

    val msgDataRDD: DStream[String] = kafkaStream.map(_._2)

    //for debug use only
    println("########## Coming data in this interval...")
    msgDataRDD.print()

    // e.g page37|5|1.5119122|-1
    val popularityData = msgDataRDD.map { msgLine => {
      val dataArr: Array[String] = msgLine.split("\\|")
      val pageID = dataArr(0)
      //calculate the popularity value
      val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
      (pageID, popValue)
      }
    }

    //sum the previous popularity value and current value
    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue:Double = t._2.sum
        val stateValue:Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }

    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))
    val stateDstream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)

    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    stateDstream.checkpoint(Duration(8*processingInterval.toInt*1000))
    //after calculation, we need to sort the result and only show the top 10 hot pages
    stateDstream.foreachRDD { rdd => {
      val sortedData = rdd.map{ case (k,v) => (v,k) }.sortByKey(false)
      val topKData = sortedData.take(10).map{ case (v,k) => (k,v) }
      topKData.foreach(x => {
        println(x)
      })
    }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
