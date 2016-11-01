package sparkstreaming.socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by admin on 2016/11/1.
  */
object StreamingByStatus {

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: <time> <hostname> <port>")
      System.exit(1)
    }

    //构造配置对象  获取系统默认的配置对象
    val conf = new SparkConf
    //构造spark streaming上下文对象  参数一是系统配置  参数二是时间间隔
    val ssc = new StreamingContext(conf,Seconds(args(0).toInt))
    //设置检查点目录
    ssc.checkpoint("/tmp")

    //参数一：发送socket消息的主机  参数二：发送socket消息的端口  参数三：存储级别
    val datas: ReceiverInputDStream[String] = ssc.socketTextStream(args(1),args(2).toInt,StorageLevel.MEMORY_ONLY)
    //业务逻辑
    val rs: DStream[(String, Int)] = datas.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    //状态累加统计
    val statusrs = rs.updateStateByKey(updateFunc)

    //打印结果集
    statusrs.print
    //启动任务  需要使用上下文对象启动
    ssc.start
    //等待任务完成
    ssc.awaitTermination

  }

  //Seq[Int]一种scala集合，可以存储重复数据，可以快速插入和删除数据数据（有序）
  //Option[Int]也是一种集合，如果有值，返回Some[A]，如果没值，返回NONE
  def updateFunc = (value:Seq[Int],status:Option[Int]) => {
    //累加当前状态
    val data = value.foldLeft(0)(_ + _)
    //取出过去的状态 初始值第一次为0
    val last = status.getOrElse(0)
    //返回
    Some(data+last)
  }

}
