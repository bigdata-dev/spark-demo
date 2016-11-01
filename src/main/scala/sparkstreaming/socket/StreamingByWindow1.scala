package sparkstreaming.socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by admin on 2016/11/1.
  */
object StreamingByWindow1 {
  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: <time> <hostname> <port> <数据时间间隔> <处理时间间隔>")
      System.exit(1)
    }

    //构造配置对象  获取系统默认的配置对象
    val conf = new SparkConf
    //构造spark streaming上下文对象  参数一是系统配置  参数二是时间间隔
    val ssc = new StreamingContext(conf,Seconds(args(0).toInt))

    //设置检查点目录  带状态的操作必须要做检查点  特别是window操作
    ssc.checkpoint("/tmp")

    //指定接收器
    //参数一：发送socket消息的主机  参数二：发送socket消息的端口  参数三：存储级别
    val datas = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_ONLY)
    //业务逻辑
    //参数一累加函数  ，参数二表示减去最初状态的函数 ， 参数三数据时间间隔 ， 参数四处理时间间隔
    val rs = datas.map((_,1)).reduceByKeyAndWindow(_ + _ , _ - _, Seconds(args(3).toInt), Seconds(args(4).toInt))
    //打印结果集
    rs.print
    //启动任务  需要使用上下文对象启动
    ssc.start
    //等待任务完成
    ssc.awaitTermination


  }

}
