package sparkcore.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/3/14.
  */
object TextLines {
  """
    |spark-submit \
    |--class sparkcore.rdd.TextLines \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TextLines")
    val sc = new SparkContext(conf)
    //通过hadoopRDD以及MapPartitionsRDD获取文件中的每一行内容
    val lines = sc.textFile("hdfs://nameservice1/library/textLines.txt")

    val lineCount = lines.map(line => (line, 1)) //每一行变成行的内容和1 变成行的tuple
    val textLines = lineCount.reduceByKey(_ + _)
    //collect 将各个节点上的数据收集到driver 变成数组 所有用pair
    //collect后array中就是一个元素 只不过这个元素是一个tuple (元祖数组)
    textLines.collect().foreach(pair => println(pair._1 + ":" + pair._2))
  }

}
