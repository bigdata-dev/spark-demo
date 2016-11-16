package sparkstreaming.kafka.lamda

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class PView(val site:String,val vistor:String,val pageUrl:String){
  override def toString = "PView(site:"+site+" vistor:"+vistor+" pageUrl:"+pageUrl+")"
}

object PView {
  def parseData(rec:String):PView ={
    val records: Array[String] = rec.split("\\|")
    PView(records.view(0),records.view(1),records.view(2))
  }
}

/**
  * Created by tangfei on 2016/11/16.
  */
object LamdaMsgComsumer {

  def processRdd(rdd: RDD[(String, String)]) = {
    val logs: RDD[String] = rdd.map(_._2)
    val pageViews: RDD[PView] = logs.map(PView.parseData(_))
    pageViews.foreach(println)
  }

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("LamdaMsgComsumer").setMaster("local")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val kafkaBrokers: String = "10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092"
    val topics: String = "lamda-topic"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokers,
      "group.id" -> "lamda-group1")
    // "auto.offset.reset" -> "smallest")

    val km = new KafkaManager(kafkaParams)
    val kafkaDStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topics))

    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(println)
        processRdd(rdd);
        //将最新消费的offset同步到zookeeper中
        km.updateZKOffsets(rdd)
      }
    })


    ssc.start()
    ssc.awaitTermination()


  }

}
