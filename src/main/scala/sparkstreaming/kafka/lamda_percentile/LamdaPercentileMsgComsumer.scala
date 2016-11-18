package sparkstreaming.kafka.lamda_percentile

import java.util

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap



/**
  * Created by tangfei on 2016/11/16.
  */
object LamdaPercentileMsgComsumer {
  private val checkPointDir = "\tmp"
  def percentile(inputDStream: InputDStream[(String, String)]) = {
   /* val mapFuncation = (String, String) => {
      val records =  rec.split("\\|")
      var loadTime = -1
      try {
        loadTime = records.view(2).toInt
      } catch {
        case e:Exception  => e.printStackTrace()
      }
      //(0)浏览器类型   (1) 页面url (2) 加载时间
      //Seq是序列，元素有插入的先后顺序，可以有重复的元素
      (Seq(records.view(0),records.view(1)),loadTime)
    }*/

   // inputDStream.map(mapFuncation)

    //组装数据预处理包
    val sourceDStream: DStream[(Seq[String], Int)] = inputDStream.map(record => {
      val s = record._2.split("\\|")
      // s(0) 代表浏览器类型，s(1)代表页面URL
      val key = Seq(s(0), s(1))
      // s(1) 代表加载时间
      val value = s(2).toInt
      (key, value)
    })

    //统计页面Seq下，按照加载时间，统计数量
    val loadTimeDStream: DStream[(Seq[String], mutable.HashMap[Int, Int])] = sourceDStream.mapPartitions(
      iter => {
        //Map Side聚集汇总结果
        //HashMap[Int,Int]  加载时间，数量
        val resultMap = new HashMap[Seq[String],mutable.HashMap[Int,Int]]
        var tmp: (Seq[String],Int) = null
        while (iter.hasNext) {
          val tmp: (Seq[String], Int) = iter.next()
          //如果tmp._1为页面Seq 为空 获取默认值
          val valueMap: mutable.HashMap[Int, Int] = resultMap.getOrElse(tmp._1, new HashMap[Int,Int])
          //tmp._2 为加载时间
          val count: Int = valueMap.getOrElse(tmp._2, 0)
          //按加载时间 累计数量
          valueMap.put(tmp._2, count + 1)
          resultMap.put(tmp._1, valueMap)
        }
        resultMap.iterator
      }
    )

    //窗口时间
    val windowTime: Duration = Seconds(15)
    //间隔时间
    val intervalTime: Duration = Seconds(3)

    //窗口函数
    val loadTimeCollectDStream: DStream[(Seq[String], mutable.HashMap[Int, Int])] = loadTimeDStream.reduceByKeyAndWindow((x: mutable.HashMap[Int, Int], y:mutable.HashMap[Int,Int]) => {
      //reduce端按照Key聚集汇总结果  [加载时间，统计数量]
      y.foreach(r => {
        x.put(r._1, x.getOrElse(r._1, 1) + r._2)
      })
      x
    }, windowTime, intervalTime)

    loadTimeCollectDStream.foreachRDD(rdd=>{println("##打印聚集后的集合");rdd.foreach(println)})

    loadTimeCollectDStream.mapPartitions(iter=>{
      //计算百分位，结果格式为 Map(key,Map(percentage,value))
      val resultMap: mutable.HashMap[Seq[String], mutable.HashMap[Double, Int]] = new HashMap[Seq[String],mutable.HashMap[Double,Int]]()
      while(iter.hasNext){
        val tmp: (Seq[String], mutable.HashMap[Int, Int]) = iter.next()


      }
      resultMap.iterator
    })




  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println( s"""
                             |Usage: LamdaPercentileMsgComsumer <brokers> <processingInterval>
                             |  <brokers> is a list of one or more Kafka brokers
                             |  <processingInterval> is  execution time interval
                             |
                             |
                             |spark-submit \\
                             |--class sparkstreaming.kafka.lamda.LamdaPercentileMsgComsumer \\
                             |--master yarn-client \\
                             |/home/ryxc/spark-jar/spark-demo.jar \\
                             |ryxc163:9092,ryxc164:9092,ryxc165:9092 5

        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(kafkaBrokers,processingInterval) = args

    val conf: SparkConf = new SparkConf().setAppName("LamdaPercentileMsgComsumer")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(processingInterval.toInt))
    ssc.checkpoint(checkPointDir)
    //val kafkaBrokers: String = "10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092"
    val topics: String = "lamda-topic"

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokers,
      "group.id" -> "lamda-group1")
    // "auto.offset.reset" -> "smallest")

    val km = new KafkaManager(kafkaParams)
    val kafkaDStream: InputDStream[(String, String)] = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics))

    percentile(kafkaDStream);

    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(println)
        //将最新消费的offset同步到zookeeper中
        km.updateZKOffsets(rdd)
      }
    })


    ssc.start()
    ssc.awaitTermination()


  }



}


