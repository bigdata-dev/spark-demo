package sparkmllib
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by tonye0115 on 2017/5/9.
  */
object kmeansDemo {
  """
    |spark-submit \
    |--class sparkmllib.kmeansDemo \
    |--master local \
    |/var/lib/hadoop-hdfs/spark-work/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    //1 构建spark对象
    val conf = new SparkConf().setAppName("kmeansDemo")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)


    //读取样本数据
    val data = sc.textFile("hdfs://nameservice1/sparkmllib/data/kmeans_data.txt")
    val parseData = data.map(s => Vectors.dense(s.split("\t").map(_.toDouble))).cache()

    //新建KMeans聚类模型，并训练
    val initMode = "k-means||"
    val numCluster = 4
    val numIterations = 100
    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(numCluster)
      .setMaxIterations(numIterations)
      .run(parseData)

    val centers = model.clusterCenters
    println("centers")
    for(i <- 0 to centers.length -1 ){
      println(centers(i)(0) + "\t" + centers(i)(1))
    }

    //误差计算
    val USSSE = model.computeCost(parseData)
    println("Within Set Sum of Squard Errors =" + USSSE)


    //保存模型
    val ModelPath = "hdfs://nameservice1/sparkmllib/model/KMeans_Model"
    model.save(sc, ModelPath)
    val sameModel = KMeansModel.load(sc, ModelPath)

    val centers2 = sameModel.clusterCenters
    println("centers2")
    for(i <- 0 to centers2.length -1 ){
      println(centers2(i)(0) + "\t" + centers2(i)(1))
    }

    println("Vectors 0.2 0.2 is belongs to clusters:" + sameModel.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))



  }
}
