package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/3/9.
  */
object TopNGroup {
  """
    |spark-submit \
    |--class sparkcore.TopNGroup \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TopNGroup")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines = sc.textFile("hdfs://nameservice1/library/topNGroup.txt", 2)
    val linesRdd = lines.map(
      line =>{
        val split = line.split("\t")
        (split(0), split(1).toInt)
      }
    )

    println("----打印rdd")
    linesRdd.collect().foreach(println)

    //对rdd进行分组
    println("----对rdd进行分组")
    val groups = linesRdd.groupByKey()

    groups.collect().foreach(println)

    //对分组内的values值排降序
    println("----对分组内的values值排降序")
    val groupSort = groups.map(tu => {
                                val key = tu._1
                                val values = tu._2
                                val sortValues = values.toList.sortWith(_ > _).take(4)
                                (key, sortValues)
                              })

    groupSort.collect().foreach(println)

    //对分组key排降序
    println("----对分组key排降序")
    groupSort.sortBy(tu => tu._1, false, 1).collect().foreach(value => {
      print(value._1)
      value._2.foreach(v=>print("\t" + v))
      println()
    })



  }
}
