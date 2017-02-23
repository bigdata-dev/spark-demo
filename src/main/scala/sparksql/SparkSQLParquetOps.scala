package sparksql

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/2/23.
  */
object SparkSQLParquetOps {

  """
    |spark-submit \
    |--class sparksql.SparkSQLParquetOps \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLParquetOps")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val sqlContext = new SQLContext(sc)

    val userDF = sqlContext.read.parquet("hdfs://nameservice1/library/examples/src/main/resources/users.parquet")

    /**
      * 注册成为临时表以供后续的sql查询
      */
    userDF.registerTempTable("users")

    /**
      * 进行数据的多维度分析
      */
    val result = sqlContext.sql("select name from users")
    result.map(t=>"Name:" + t(0)).collect().foreach(println)

    ///////////////////////////////////////////////////////////////////
    /////////////////////////Schema Merging////////////////////////////
    ///////////////////////////////////////////////////////////////////
    import sqlContext.implicits._

    val df1 = sc.makeRDD(1 to 5).map(i => (i, i*2)).toDF("single", "double")
    df1.show()
    df1.write.parquet("data/test_table/key=1")

    val df2 = sc.makeRDD(6 to 10).map(i => (i, i*3)).toDF("single", "triple")
    df2.show()
    df2.write.parquet("data/test_table/key=2")

    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
    df3.printSchema()
    df3.show()




  }

}
