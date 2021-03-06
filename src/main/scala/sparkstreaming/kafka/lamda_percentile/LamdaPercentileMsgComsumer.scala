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

    loadTimeCollectDStream.foreachRDD(rdd=>{println("##打印聚集后的集合:");rdd.foreach(println)})

    val loadTimePartitions: DStream[(Seq[String], mutable.HashMap[Double, Int])] = loadTimeCollectDStream.mapPartitions(iter => {
      //计算百分位，结果格式为 Map(key,Map(percentage,value))
      val resultMap: mutable.HashMap[Seq[String], mutable.HashMap[Double, Int]] = new HashMap[Seq[String],mutable.HashMap[Double,Int]]()
      while (iter.hasNext) {
        val tmp: (Seq[String], mutable.HashMap[Int, Int]) = iter.next()
        val map: mutable.HashMap[Int, Int] = tmp._2
        //按照页面统计访问次数
        val sumCount: Int = map.map(r => r._2).reduce(_ + _)
        println("##sumCount:"+sumCount)

        //计算占比的访问次数
        val p25: Double = sumCount * 0.25
        val p50: Double = sumCount * 0.5
        val p70: Double = sumCount * 0.75
        println("p25:"+p25)
        println("p50:"+p50)
        println("p70:"+p70)
        val sortDataSeq: Seq[(Int, Int)] = map.toSeq.sortBy(r => r._1)

        val iters: Iterator[(Int, Int)] = sortDataSeq.iterator
        var curTmpSum = 0.0
        var prevTmpSum = 0.0
        val valueMap: mutable.HashMap[Double, Int] = new HashMap[Double,Int]
        //循环计算 每个页面下 超时时间的次数 占 每个页面总次数的 百分比区间
        while (iters.hasNext) {
          val tmpData: (Int, Int) = iters.next()
          prevTmpSum = curTmpSum
          curTmpSum += tmpData._2
          //println("##---- prevTmpSum:" + prevTmpSum)
          println("##---- curTmpSum:" + curTmpSum)
          println("##---- tmpData._1:" + tmpData._1)

         //上一次累加小于于等于p25  这一次累加大于等p25
         if (prevTmpSum <= p25 && curTmpSum >= p25) {
            println("p25------- prevTmpSum:" + prevTmpSum + "----curTmpSum:" + curTmpSum + "--tmpData._1:" + tmpData._1)
            valueMap.put(0.25, tmpData._1)
           //上一次累加小于于等于p50  这一次累加大于等p50
          }else if (prevTmpSum <= p50 && curTmpSum >= p50) {
            println("p50------- prevTmpSum:" + prevTmpSum + "----curTmpSum:" + curTmpSum + "--tmpData._1:" + tmpData._1)
            valueMap.put(0.50, tmpData._1)
           //上一次累加小于于等于p70  这一次累加大于等p70
          }else if (prevTmpSum <= p70 && curTmpSum >= p70) {
            println("p70------- prevTmpSum:" + prevTmpSum + "----curTmpSum:" + curTmpSum + "--tmpData._1:" + tmpData._1)
            valueMap.put(0.70, tmpData._1)
          }
        }
        resultMap.put(tmp._1,valueMap)
      }
      resultMap.iterator
    })
    loadTimePartitions.foreachRDD(x=>{println("##打印计算百分位后的集合:");x.foreach(println)})




  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println( s"""
                             |Usage: LamdaPercentileMsgComsumer <brokers> <processingInterval>
                             |  <brokers> is a list of one or more Kafka brokers
                             |  <processingInterval> is  execution time interval
                             |
                             |
                             |spark-submit \
                             |--class sparkstreaming.kafka.lamda_percentile.LamdaPercentileMsgComsumer \
                             |--master local \
                             |/home/ryxc/spark-jar/spark-demo.jar \
                             |ryxc163:9092,ryxc164:9092,ryxc165:9092 1
                             |
                             |
                             |spark-submit \
                             |--jars /opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/jars/spark-streaming-kafka_2.10-1.6.0-cdh5.7.1.jar,/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/jars/kafka_2.10-0.9.0-kafka-2.0.0.jar,/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/jars/kafka-clients-0.9.0-kafka-2.0.0.jar,/opt/cloudera/parcels/CDH-5.7.1-1.cdh5.7.1.p0.11/jars/metrics-core-2.2.0.jar \
                             |--class sparkstreaming.kafka.lamda_percentile.LamdaPercentileMsgComsumer \
                             |--master local \
                             |/root/spark-work/spark-demo.jar \
                             |10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092 1

        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(kafkaBrokers,processingInterval) = args

    val conf: SparkConf = new SparkConf().setAppName("LamdaPercentileMsgComsumer")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(processingInterval.toInt))
    ssc.checkpoint(checkPointDir)
    //val kafkaBrokers: String = "10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092"
    val topics: String = "lamda-percentile-topic"

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


