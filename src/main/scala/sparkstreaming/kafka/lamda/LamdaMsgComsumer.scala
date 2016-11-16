package sparkstreaming.kafka.lamda

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
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
  private val checkPointDir = "."
  def processRdd(rdd: RDD[(String, String)]) = {
    val logs: RDD[String] = rdd.map(_._2)
    val pageViews: RDD[PView] = logs.map(PView.parseData(_))
    pageViews.foreach(println)

    //(1）统计每批次指定时间段数据PV
    val pageCounts: scala.collection.Map[String, Long] = pageViews.map(view=>view.pageUrl).countByValue()
    pageCounts.foreach(println)

  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop2.6.0_util")
    val conf: SparkConf = new SparkConf().setAppName("LamdaMsgComsumer").setMaster("local")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint(checkPointDir)
    val kafkaBrokers: String = "10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092"
    val topics: String = "lamda-topic"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokers,
      "group.id" -> "lamda-group1")
    // "auto.offset.reset" -> "smallest")

    val km = new KafkaManager(kafkaParams)
    val kafkaDStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topics))

    val pageViewsDStream: DStream[PView] = kafkaDStream.map(_._2).map(PView.parseData(_))
    //(2）统计过去15s的访客数量，每隔3s计算一次
    val windowTime: Duration = Seconds(15)
    val intervalTime: Duration = Seconds(3)

    val vistorCounts: DStream[(String, Int)] = pageViewsDStream.window(windowTime,intervalTime).map(view=>(view.vistor,1)).groupByKey().map(v=>(v._1,v._2.size))
    vistorCounts.foreachRDD(rdd=>{
      rdd.foreach(x=>{println("#####window:"+x)})
    })

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
