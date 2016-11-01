package sparkstreaming.hdfs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by admin on 2016/11/1.
  */
object StreamingHDFS {
  def main(args: Array[String]) {

    if(args.length!=2){
      println("Usage:<time> <inputpath")
      System.exit(1)
    }
    //构造配置对象  获取系统默认的配置对象
    val conf: SparkConf = new SparkConf
    //构造spark streaming上下文对象  参数一是系统配置  参数二是时间间隔
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(args(0).toInt))
    //指定接收器  参数为hdfs的目录
    val datas: DStream[String] = ssc.textFileStream(args(1))
    //业务逻辑
    val rs: DStream[(String, Int)] = datas.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

    //打印结果集
    rs.print()

    //启动任务  需要使用上下文对象启动
    ssc.start()

    //等待任务完成
    ssc.awaitTermination()


  }

}
