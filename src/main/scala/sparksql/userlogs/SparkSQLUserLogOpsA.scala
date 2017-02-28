package sparksql.userlogs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by tonye0115 on 2017/2/27.
  */
object SparkSQLUserLogOpsA {

  val conf = new SparkConf().setAppName("SparkSQL2Hive")
  val sc = new SparkContext(conf)

  /**
    * 以下代码是在 spark-shell命令下执行
    * 因为CDH没有spark-sql命令，只能借助spark-shell在hiveContext中执行
    */
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.sql("DROP TABLE IF EXISTS  userLogs")
  hiveContext.sql("CREATE TABLE IF NOT EXISTS  " +
    "userLogs(date STRING, timestamp BIGINT, userID INT,pageID INT,channel STRING,action  STRING) " +
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE")

  hiveContext.sql("DESC formatted  userLogs").show()

  hiveContext.sql("LOAD DATA LOCAL INPATH  '/var/lib/hadoop-hdfs/tmp/userLog.log'" +
    " INTO TABLE userLogs")


}
