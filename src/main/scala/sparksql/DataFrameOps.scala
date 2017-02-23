package sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/2/21.
  *
  */
object DataFrameOps {
  """
    |spark-submit \
    |--class sparksql.DataFrameOps \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Spark hello!")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://nameservice1/library/examples/src/main/resources/people.json")
    df.show()
    df.printSchema()

    df.select("name").show()
    df.select(df("name"), df("age") + 10).show()
    df.filter(df("age") > 10).show()
    df.groupBy("age").count().show()

  }

}
