package sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/2/24.
  */
object SparkSQLWindowfuncationOps {
  """
    |spark-submit \
    |--class sparksql.SparkSQLWindowfuncationOps \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLInnerFuncations")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    sqlContext.sql("DROP TABLE IF EXISTS scores")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS  scores(name STRING, score INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE")
    sqlContext.sql("LOAD DATA LOCAL INPATH  '/var/lib/hadoop-hdfs/tmp/groupTopN.txt'" +
      " INTO TABLE scores")

    /**
      * 使用窗口函数row_number来进行分组排序
      * PARTITION BY：指定窗口函数分组的key
      * ORDER by :分组和进行排序
      */
    val result = sqlContext.sql("SELECT name,score FROM " +
      "(SELECT name, " +
      "score, " +
      "row_number() OVER (PARTITION BY name ORDER BY score DESC) rank " +
      "FROM scores) sub_scores " +
      "WHERE rank <= 2  ")

    result.show()

    sqlContext.sql("DROP TABLE IF EXISTS  sortedResultScores")
    result.saveAsTable("sortedResultScores")



  }
}
