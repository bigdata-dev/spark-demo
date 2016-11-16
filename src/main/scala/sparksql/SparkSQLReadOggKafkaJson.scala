package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tangfei on 2016/11/11.
  */
object SparkSQLReadOggKafkaJson {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop2.6.0_util")
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkSQLReadOggKafkaJson")
    conf.setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    //构造hiveContext对象或者sqlContext对象
   // val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    //读取结构化数据源json文件
    val file: RDD[String] = sc.textFile(this.getClass.getResource("/").getPath+"ogg-kafka-jsonN.txt")
    var map: RDD[String] = file.flatMap(_.split("}}")).map(_.concat("}}"))
    //map.foreach(println)

    val SchemaRDD: DataFrame = sqlContext.jsonRDD(map)
    //println(SchemaRDD)

    //打印SchemaRDD结构
    SchemaRDD.printSchema

    //注册成为一张表
    SchemaRDD.registerTempTable("info")

    //使用sql查询临时表
   // val rs = hiveContext.sql("select * from info where before.BILL_CODE = '417696335296'")
    val rs = sqlContext.sql("select count(*) from info where" +
      " from_unixtime(unix_timestamp(before.CREATE_DATE),'yyyyMMdd')='20161111'" )

    rs.foreach(println)
    //sqlContext.sql("select  before.CREATE_DATE,from_unixtime(unix_timestamp(before.CREATE_DATE),'yyyyMMdd') from info ").foreach(println)
    sqlContext.sql("select  before.CREATE_DATE,hour(before.CREATE_DATE) from info  ").foreach(println)
  }

}
