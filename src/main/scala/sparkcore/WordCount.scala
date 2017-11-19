package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/3/10.
  */
object WordCount {
  """
    |spark-submit \
    |--master yarn \
    |--deploy-mode client \
    |--class sparkcore.WordCount \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-work/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")

    val lines = sc.textFile("hdfs://nameservice1/library/wordcount.txt")
    lines.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_+_, 1).collect().foreach(println)
    sc.stop()
  }

}
