package sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/2/24.
  */
object SparkSQL2Hive {
  """
    |spark-submit \
    |--class sparksql.SparkSQL2Hive \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQL2Hive")
    val sc = new SparkContext(conf)
    /**
      * 1.基于HiveContext我们可以使用sql/hql两种方式编写sql语句对hive进行操作
      * 2.可以直接通过saveAsTable的方式把dataframe中的数据保存到hive中
      * 3.可以直接通过HiveContext.table来直接加载hive中的表生成dataframe
      */
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("DROP TABLE IF EXISTS  people")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS  people(name STRING, age INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' STORED AS TEXTFILE")
    hiveContext.sql("LOAD DATA LOCAL INPATH  '/var/lib/hadoop-hdfs/people.txt'" +
      " INTO TABLE people")

    hiveContext.sql("DROP TABLE IF EXISTS  peoplescores")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS  peoplescores(name STRING, scores INT)" +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' STORED AS TEXTFILE")
    hiveContext.sql("LOAD DATA LOCAL INPATH  '/var/lib/hadoop-hdfs/peoplescores.txt'" +
      " INTO TABLE peoplescores")


    val resultDF = hiveContext.sql(
      """
        |FROM people pi JOIN peoplescores ps ON pi.name=ps.name
        |SELECT pi.name,pi.age,ps.scores
      """.stripMargin
    )

    /**
      * 1.通过savaAsTable创建一张Hive Managed Table
      * 2.默认存储在hdfs上的格式是 .parquet
      */
    hiveContext.sql("DROP TABLE IF EXISTS  peopleinformatresult")
    resultDF.saveAsTable("peopleinformatresult")


    /**
      * 1.使用HiveContext的table方面可以直接去读Hive数据仓库中的table并生成DataFrame
      * 2.接下来可以进行机器学习，图计算，各种复杂的ETL等操作
      */
    val dataFromHive = hiveContext.table("peopleinformatresult")
    dataFromHive.show()






  }

}
