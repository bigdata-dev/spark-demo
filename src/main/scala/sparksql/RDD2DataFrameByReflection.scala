package sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tonye0115 on 2017/2/21.
  * 使用反射方式将RDD转换成为DataFrame
  */
object RDD2DataFrameByReflection {
  """
    |spark-submit \
    |--class sparksql.RDD2DataFrameByReflection \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("RDD2DataFrameByReflection")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //反射persion所有的fields和rdd 生成dataFrame
    val people = sc.textFile("hdfs://nameservice1/library/examples/src/main/resources/people.txt")
                        .map(_.split(",")).map(p=>Person(p(0), p(1).trim.toInt)).toDF()

    people.show()

    //将dataFrame注册为临时表
    people.registerTempTable("people")

    val teenagers = sqlContext.sql("SELECT name,age FROM people WHERE age >= 13 and age <=19")

    //sql查询的结果是dataFrame支持标准的rdd操作
    //by field index (注意:dataFrame会自动排序，所有按索引取的时候出问题)
    teenagers.map(t=>"name:"+t(0)).collect().foreach(println)
    //by field name
    teenagers.map(t=>"name:"+t.getAs[String]("name")).collect().foreach(println)
    //row.getValuesMap Map("name" -> "Justin", "age" -> 19)
    teenagers.map(_.getValuesMap[Any](List("name","age"))).collect().foreach(println)




  }

}

case class Person(name:String, age:Int)