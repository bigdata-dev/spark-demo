package sparkstreaming.kafka.consumer

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import utils.MDBManager

/**
  * Created by tangfei on 2016/10/28.
  */
object WebPagePopularityValueCalculatorJdbc {
  private val isLocal = true
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
                             |
                             |kafka-console-producer --broker-list ryxc164:9092 --topic user-behavior-topic
                             |
                             |spark-submit \\
                             |--class sparkstreaming.kafka.consumer.WebPagePopularityValueCalculator \\
                             |--master yarn-client \\
                             |/home/ryxc/spark-jar/spark-demo.jar \\
                             |ryxc163:9092,ryxc164:9092,ryxc165:9092 user-behavior-topic user-behavior-topic-message-consumer-group2 5

        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers, topics, groupId,processingInterval) = args

    // Create context with (processingInterval) second batch interval
    val conf = new SparkConf().setAppName("WebPagePopularityValueCalculatorJdbc")
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

    //打印从kafka接入的消息
    kafkaStream.foreachRDD(rdd => {
      println("\n\n##Number of records in this batch : " +rdd.count())
      //rdd.collect.map(_._2).foreach(println)
    })

    val msgDataRDD: DStream[String] = kafkaStream.map(_._2)
    //for debug use only
    println("Coming data in this interval...")
    //msgDataRDD.print()

    // e.g page37|5|1.5119122|-1
    val popularityData: DStream[(String, Double)] = msgDataRDD.map { msgLine => {
        val dataArr: Array[String] = msgLine.split("\\|")
        val pageID = dataArr(0)
        //calculate the popularity value
        val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
        (pageID, popValue)
      }
    }

    //popularityData.foreachRDD(rdd=>rdd.collect.foreach(println))

    val initalRDD: RDD[(String, Double)] = ssc.sparkContext.parallelize(List(("page1",0.00)))
    val updatePopularityValue = (iterator:Iterator[(String,Seq[Double],Option[Double])]) => {
      iterator.flatMap(t=>{
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0)
        Some(newValue + stateValue)
      }.map(sumedValue=>(t._1,sumedValue)))
    }


    //调用 updateStateByKey 原语并传入上面定义的匿名函数更新网页热度值。
    //val stateDStream: DStream[(String, Double)] = popularityData.updateStateByKey(updatePopularityValueFunc)
    val stateDStream: DStream[(String, Double)] = popularityData.updateStateByKey(
      updatePopularityValue,new HashPartitioner(ssc.sparkContext.defaultParallelism),true,initalRDD)

    stateDStream.foreachRDD(rdd=>{
      println("\n\nStateByKey:"+rdd.count())
      //rdd.collect.foreach(println)  //使用collect效率很低的
    })

    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    stateDStream.checkpoint(Duration(8*processingInterval.toInt*1000))

    //最后得到最新结果后，需要对结果进行排序，最后打印热度值最高的 10 个网页。
    stateDStream.foreachRDD(rdd => {
      println("\n\n##foreachPartition store:")
      rdd.foreachPartition(data=>{
        val topKData: Array[(String, Double)] = data.toArray.sortWith(_._2 > _._2).take(10)
        println("\n\nTopKData:")
        topKData.foreach(println)

        //从连接池中获取一个连接
        val conn = MDBManager.getMDBManager(isLocal).getConnection
        conn.setAutoCommit(false)
        val deleteSql = "delete from popularity_topn"
        val deletePreparedStatement = conn.prepareStatement(deleteSql)
        deletePreparedStatement.execute()
        val sql = "insert into popularity_topn set page_name=?,popularity=?"
        val preparedStatement = conn.prepareStatement(sql)
        topKData.foreach(r => {
          preparedStatement.setObject(1, r._1)
          preparedStatement.setObject(2, r._2)
          preparedStatement.addBatch()
        })
        //批量提交，如果数据量大，这里可以分批提交
        preparedStatement.executeBatch()
        conn.commit()
        conn.close()
      })
    })

    /**
      * 将消费group的offset更新到zookeeper中
      *
      */
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(println)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def updatePopularityValueFunc=(value:Seq[Double],status:Option[Double])=>{
    val newValue: Double = value.sum
    val stateValue: Double = status.getOrElse(0)
    Some(newValue+stateValue)
  }


  def processRdd(rdd: RDD[(String, String)]) = {
  }

}
