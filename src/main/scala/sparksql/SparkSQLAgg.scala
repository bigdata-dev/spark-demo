package sparksql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by tonye0115 on 2017/2/24.
  * 使用sparksql中的内置函数对数据进行分析，与普通的sparksql Api不同的是
  * dataFrame中的内置函数的操作的结果是返回一个column对象
  * dataFrame采用列式存储，
  * A distributed collection of data organized into named columns.
  * 为数据复杂分析建立的了坚实的基础
  * 并提供了极大的方便性，
  * 例如, 我们在操作DataFrame的方法中可以随时调用内置函数进行业务需求处理，
  * 这之于我们构建复杂的业务逻辑而极大的减少不必须的时间消耗（基于时间模型的映射）
  * 让我们聚焦在数据分析上，这对于提搞生产力而言是非常有价值的
  *
  * spark1.5开始提供了大量的内置函数 例如
  * agg max mean min max sum avg explode size sort_array day to_data abs
  * 总体而言，内置函数包含5大基本类型：
  * 1.聚合函数  countDistinct, sumDistinct等
  * 2.集合函数  sort_array, explode等
  * 3.日期、时间函数  hour, quarter, next_day等
  * 4.数据函数  asin, atan, sqrt, tan, round等
  * 5.窗口函数  rowNumber等
  * 6.字符串函数 concat, format_numbe，rexexp_extract等
  * 7.其他函数 isNaN, sha, randn, callUDF, UDAF
  */
object SparkSQLAgg {
  """
    |spark-submit \
    |--class sparksql.SparkSQLAgg \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLInnerFuncations")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    //使用时sparksql的内置函数，就一定要导入sqlContext下的隐式转换
    import sqlContext.implicits._

    val userData = Array(
      "2016-3-27,001,http://spark.apache.org/,1000",
      "2016-3-27,001,http://hadoop.apache.org/,1001",
      "2016-3-27,002,http://flink.apache.org/,1002",
      "2016-3-28,003,http://kafka.apache.org/,1020",
      "2016-3-28,004,http://spark.apache.org/,1010",
      "2016-3-28,002,http://hive.apache.org/,1200",
      "2016-3-28,001,http://parquet.apache.org/,1500",
      "2016-3-28,001,http://spark.apache.org/,1800"
    )

    val userDataRDD = sc.parallelize(userData) //生成RDD分布式结合对象

    /**
      * 对数据进行处理生成dataframe,
      * 需要把RDD生成DataFrame
      * 需要先把RDD中的元素类型变成Row类型
      * 于此同时要提供DataFrame中的columns的元素信息描述：
      */
    val userDataRDDRow = userDataRDD.map(row => {
      val splited = row.split(",")
      Row(splited(0), splited(1).toInt, splited(2), splited(3).toInt)
    })

    val structTypes = StructType(Array(
      StructField("time", StringType, true),
      StructField("id", IntegerType, true),
      StructField("url", StringType, true),
      StructField("amount", IntegerType, true)
    ))

    val userDataDF = sqlContext.createDataFrame(userDataRDDRow, structTypes)

    /**
      * 使用sparkSql提供的内置函数对dataframe进行操作
      * 注意：
      * 内置函数生成的column对象且自定进行CG
      */
    //对每天访问用户统计
    userDataDF.groupBy("time").agg('time, countDistinct('id))
      .map(row => Row(row(1), row(2))).collect().foreach(println)

    //对销售额进行统计 所有的内置函数操作都会返回具体的列
    userDataDF.groupBy("time").agg('time, sum('amount)).show()







  }

}
