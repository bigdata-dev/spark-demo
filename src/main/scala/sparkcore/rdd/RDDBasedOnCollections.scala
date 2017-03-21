package sparkcore.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/3/12.
  */
object RDDBasedOnCollections {
  """
    |spark-submit \
    |--class sparkcore.rdd.RDDBasedOnCollections \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDBasedOnCollections")
    val sc = new SparkContext(conf)
    //创建一个scala集合
    val numbers = 1 to 100
    val rdd = sc.parallelize(numbers, 10)

    //类型推导
    val sum = rdd.reduce(_ + _)

    println("sum:"+sum)
  }
}
