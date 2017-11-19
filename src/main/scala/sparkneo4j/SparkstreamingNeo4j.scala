package sparkneo4j

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.neo4j.spark._

/**
  */
object SparkstreamingNeo4j {

  def main(args: Array[String]) {
    """
      |spark-submit \
      |--master yarn \
      |--deploy-mode client \
      |--executor-memory 2G \
      |--conf spark.neo4j.bolt.url=bolt://192.168.116.191:7687 \
      |--conf spark.neo4j.bolt.user=neo4j \
      |--conf spark.neo4j.bolt.password=neo4j  \
      |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-spark-connector-1.0.0-RC1.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/fastjson-1.2.40.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/graphframes-0.1.0-spark1.6.jar \
      |--class sparkneo4j.SparkstreamingNeo4j \
      |/var/lib/hadoop-hdfs/spark-work/spark-demo.jar \
      |cdh20.ryxc:9092,cdh21.ryxc:9092,cdh22.ryxc:9092 zto_path zto_path-consumer-group 5
      |
      |
    """.stripMargin
    //Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers, topics, groupId,processingInterval) = args

    val conf = new SparkConf().setAppName("SparkstreamingNode4j")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf,Seconds(processingInterval.toInt))
    val sc = ssc.sparkContext
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

    kafkaStream.foreachRDD(rdd => {
      println("\n\nNumber of records in this batch : " +rdd.count())
      rdd.collect.map(_._2).foreach(x=>{println(x+"--")})

      if (!rdd.isEmpty()) {
        // 先处理消息
        processRdd(rdd)
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })

    //启动
    ssc.start()
    ssc.awaitTermination()


    def processRdd(rdd: RDD[(String, String)]) = {
      rdd.foreachPartition(data=>{

//        val jsonObject = com.alibaba.fastjson.JSON.parseObject(data.map(_._2).toString())
//        val bill_code = jsonObject.get("bill_code")
//        val ds = jsonObject.get("ds")
//        val array = jsonObject.get("data").asInstanceOf[com.alibaba.fastjson.JSONArray]
//        for (i <- 0 to array.size()-1) {
//          val path_data = array.get(i).asInstanceOf[com.alibaba.fastjson.JSONObject]
//          val start_node_name = path_data.get("start_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("name")
//          val start_node_category_code = path_data.get("start_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("category_code")
//          val end_node_name = path_data.get("end_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("name")
//          val end_node_category_code = path_data.get("end_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("category_code")
//          val start_time = path_data.get("start_time")
//          val end_time = path_data.get("end_time")
//          val cost_time = path_data.get("cost_time")
//
//          crate_node(sc,start_node_name,start_node_category_code)
//          crate_node(sc,end_node_name,end_node_category_code)
//          create_relation(sc, ds, bill_code, start_node_name, end_node_name,start_time,end_time, cost_time)
//        }
      })


    }


//    def crate_node(sc:org.apache.spark.SparkContext, node_name:AnyRef, category_code:AnyRef): Unit ={
//      val cql = "merge (n:Node {name:{start_node_name},category_code:{category_code}}) on create set n.created = timestamp()"
//      val params = scala.Seq("start_node_name" -> node_name, "category_code" -> category_code)
//      Neo4jGraph.execute(sc,cql,params)
//    }

//    def create_relation(sc:org.apache.spark.SparkContext, ds:AnyRef, bill_code:AnyRef, start_node_name:AnyRef, end_node_name:AnyRef, start_time:AnyRef, end_time:AnyRef, cost_time:AnyRef): Unit ={
//      val cql = "match (a:Node {name:{start_node_name}}),(b:Node {name:{end_node_name}}) " +
//        "merge (a) -[r:b_"+bill_code+" {start_time: {start_time},end_time: {end_time},cost_time: {cost_time},bill_code: {bill_code},ds: {ds}}]->(b);"
//      val params = scala.Seq("ds" ->ds, "bill_code" -> bill_code,"start_node_name" -> start_node_name,"end_node_name" -> end_node_name,"start_time" -> start_time, "end_time" -> end_time, "cost_time" -> cost_time)
//      Neo4jGraph.execute(sc,cql,params)
//    }


  }

}
