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
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  sqlContext.sql("DROP TABLE IF EXISTS  userLogs")
  sqlContext.sql("CREATE TABLE IF NOT EXISTS  " +
    "userLogs(date STRING, timestamp BIGINT, userID INT,pageID INT,channel STRING,action  STRING) " +
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE")

  sqlContext.sql("DESC formatted  userLogs").show()

  sqlContext.sql("LOAD DATA LOCAL INPATH  '/var/lib/hadoop-hdfs/tmp/userLog.log'" +
    " INTO TABLE userLogs")

  //统计pv  (每个页面有多少次访问)
  sqlContext.sql("select date,pageID, pv from ( " +
    " select ul.date, ul.pageId, count(*) as pv " +
    " from userLogs ul " +
    " where ul.action='View' and ul.date='2017-02-26' group by ul.date,ul.pageID ) subquery" +
    " order by pv desc ").show()




  //统计uv  (每个页面有多少个用户访问)
  sqlContext.sql("select date,pageID, pv from ( " +
    " select ul.date, ul.pageId, count(distinct(userID)) as pv " +
    " from userLogs ul " +
    " where ul.action='View' and ul.date='2017-02-26' group by ul.date,ul.pageID ) subquery" +
    " order by pv desc ").show()



  //统计总pv collect 返回的是org.apache.spark.sql.ROW
  val pvCount = sqlContext.sql("select count(*) from userLogs ul" +
    " where ul.action='View' and ul.date='2017-02-26'").collect()

  val totalTargetPV = pvCount(0).get(0)

  //用户跳出率 (今天访问页面只有一次)
  val targetResult =  sqlContext.sql("select count(*) from (select count(*) totalNumber from userLogs where action = 'View' and date='2017-02-26'" +
    " group by userID having totalNumber=1) targetTable ").collect()
  val pv1 = targetResult(0).get(0)
  val percent = pv1.toString.toDouble/totalTargetPV.toString.toDouble






}
