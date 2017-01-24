package sparksql

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tangfei on 2016/11/11.
  */
object SparkSQLReadOggKafkaJson2 {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkSQLReadOggKafkaJson2")
    conf.setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(1))

    val kafkaBrokers = "ryxc163:9092,ryxc164:9092,ryxc165:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokers,
      "group.id" -> "kafkaReadGroup2")
    // "auto.offset.reset" -> "smallest")

    val topic: String = "zto_order" // 消费的 topic 名字
    val topics = Set(topic)

    val km = new KafkaManager(kafkaParams)
    val kafkaDStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    kafkaDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(println)
        processRdd1(rdd);
        //将最新消费的offset同步到zookeeper中
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def processRdd1(rdd:RDD[(String, String)]):Unit = {
    val sqlContext:SQLContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val record: RDD[String] = rdd.map(_._2).flatMap(_.split("}}")).map(_.concat("}}"))
    sqlContext.read.json(record).registerTempTable("ogg_zto_order")
    val rs = sqlContext.sql(
      """
        |select op_ts,after.CREATE_DATE from ogg_zto_order limit 1
      """.stripMargin
    )
    rs.foreach(println)
    rs.show()
  }
}


object SQLContextSingleton{
  @transient private var instance:SQLContext = _
  def getInstance (sparkContext: SparkContext):SQLContext = {
    if(instance == null){
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
