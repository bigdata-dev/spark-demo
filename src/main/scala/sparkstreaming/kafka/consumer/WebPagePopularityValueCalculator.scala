package sparkstreaming.kafka.consumer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tangfei on 2016/10/28.
  */
object WebPagePopularityValueCalculator {
  def main(args: Array[String]) {
    if(args.length<2){
      println("Usage:WebPagePopularityValueCalculator zk1 zk2")
      System.exit(1)
    }

    val Array(zkServers,processingInterval) = args;

    val conf = new SparkConf().setAppName("WebPagePopularityValueCalculator")
    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))

  }

}
