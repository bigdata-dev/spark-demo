package sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/2/24.
  *
  * UDF: 用户自定义函数，函数的输入时一条具体的数据记录，实际上是普通的scala函数
  * UDAF: 用户自定义的聚合函数，函数本身作用于数据集合，能够在聚合操作的基础上进行自定义操作
  *
  * 实质上， UDF会被spark sql 中的catalyst封装成为Expression,最终会通过eval方法来计算输入的数据ROW
  * 此处的Row和DataFrame中的Row没有任何关系
  */
object SparkSQLUDFUDAF {
  """
    |spark-submit \
    |--class sparksql.SparkSQLUDFUDAF \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLInnerFuncations")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    val bigData = Array("Spark", "Spark", "Hadoop", "Spark","Hadoop",
    "Spark","Spark","Hadoop","Spark","Hadoop")

    /**
      * 基于提供的数据创建DataFrame
      */
    val bigDataRDD = sc.parallelize(bigData)
    val bigDataRDDRow = bigDataRDD.map(item => Row(item))
    val structType = StructType(Array(StructField("word", StringType, true)))
    val bigDataDF = sqlContext.createDataFrame(bigDataRDDRow, structType)

    bigDataDF.registerTempTable("bigDataTable") //注册临时表

    /**
      * 通过sqlContext注册UDF，在scala 2.10.x版本UDF函数可以接受22个输入参数
      */
    sqlContext.udf.register("computeLength", (input:String) => input.length)

    sqlContext.sql("select word, computeLength(word) as length from bigDataTable ").show()

    sqlContext.udf.register("wordCount", new MyUDAF)

    //查询出有几种类型的word 以及word出现多少次 以及这个word有多长
    sqlContext.sql("select word, wordCount(word) as count,computeLength(word) as length" +
      " from bigDataTable " +
      " group by word ").show()



    while(true)()


  }

}

/**
  * 按照模板实现UDAF
  */
class MyUDAF extends UserDefinedAggregateFunction{
  /**
    * 具体指定输入数据的类型
    * StructField指定的列名和输入的没关系
    * @return
    */
  override def inputSchema: StructType =
    StructType(Array(StructField("input",StringType,true)))


  /**
    * 在进行聚合操作的时候，所要处理的数据的结果类型
    */
  override def bufferSchema: StructType =
    StructType(Array(StructField("count", IntegerType, true)))

  /**
    * 指定UDAF函数计算后返回的结果类型
    * @return
    */
  override def dataType: DataType = IntegerType

  /**
    * 确保一致性
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 在Aggregate之前每组数据的初始化结果
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0)}


  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何计算
    * 相当于Hadoop MapReduce模型中的Combiner
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  /**
    * 最后在分布式节点进行local reduce完成后需要进行全局级别的merger的操作
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }


  /**
    * 返回UDAF最后的计算结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)


}

