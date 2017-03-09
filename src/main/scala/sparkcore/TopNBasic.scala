package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/3/8.
  * 基础topN 案例实战
  */
object TopNBasic {
  """
    |spark-submit \
    |--class sparkcore.TopNBasic \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TopNBasic")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    //读文件设置为一个partition
    val lines = sc.textFile("hdfs://nameservice1/library/basicTopN.txt")

    //生成key-value 键值对 以方便sortbyKey进行排序
    val pairs = lines.map(line => (line.toInt, line))
    //降序排序
    val sortedPairs = pairs.sortByKey(false)
    //只要是改变一行列的数据 都用map  过滤出排序后的内容本身
    val sortedData = sortedPairs.map(pair => pair._2)
    //获取排名前五的元素内容
    val top5 = sortedData.take(5)
    //打印数据
    top5.foreach(println)
  }

}
