package sparksql

import java.sql.DriverManager
import java.util

import java.sql.{DriverManager, PreparedStatement, Connection}
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/2/21.
  */
object SparkSQLJDBC2MySQL {


  """
    |spark-submit \
    |--class sparksql.SparkSQLJDBC2MySQL \
    |--master yarn-client \
    |--driver-memory 1024m \
    |--executor-memory 1024m \
    |--jars /usr/share/java/mysql-connector-java.jar  \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLJDBC2MySQL")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)

    /**
      * 通过format("jdbc")的方式说明sparkSQL操作的数据来源是通过JDBC获得的
      */
    val reader = sqlContext.read.format("jdbc")
    reader.option("url", "jdbc:mysql://hadoop10.ryxc:3306/test")
    reader.option("dbtable", "people")
    reader.option("driver", "com.mysql.jdbc.Driver")
    reader.option("user", "root")
    reader.option("password", "root")

    /**
      * 在实际的企业级开发环境中，如果数据库中数据库规模特别大
      * 例如 10亿条数据，此时采用传统的DB去处理的话
      * 一般需要对10亿条数据分成很多批次处理，例如分成100批（受限于单台的Server的处理能力）
      * 且实际的处理的过程中可能会非常复杂，通过传统的Java EE等技术可能很难或者不方便处理
      * 此时采用sparksql获取数据库中的数据并进行分布式处理可以非常好的解决该问题
      * 但是由于sparksql加载DB中的数据需要时间，所以一般会在sparksql和
      * 具体要操作的DB直接加上一个缓冲层次：例如中间使用redis，可以把spark处理速度提高到45倍
      */
    val df1 = reader.load()

    df1.show()

    reader.option("dbtable", "dtsell")

    val df2 = reader.load()
    df2.show()

    val res = df1.join(df2, df1("name")===df2("name"), "left")
      .select(df1("name"),df1("age"),df2("goods"))
    res.show()

    println("================================================")
    //一般用法：将dataFrame转rdd后在存入数据库
    res.toJavaRDD.foreachPartition(new VoidFunction[util.Iterator[Row]] {
      override def call(t: util.Iterator[Row]): Unit ={
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = "insert into result(name, age,goods) values (?, ?, ?)"
        try {
          //不同的数据分片要单独建立连接 所以写到call里面
          conn = DriverManager.getConnection("jdbc:mysql://hadoop10.ryxc:3306/test", "root", "root")
          while(t.hasNext){
            val row = t.next()
            ps = conn.prepareStatement(sql)
            ps.setString(1, row.getAs[String]("name"))
            ps.setInt(2, row.getAs[Int]("age"))
            ps.setString(3, row.getAs[String]("goods"))
            ps.executeUpdate()
          }
        } catch {
          case e: Exception => println("Mysql Exception")
        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }

      }
    })

    sc.stop()

  }





}
