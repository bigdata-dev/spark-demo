package sparkmllib


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/5/8.
  */
object tree {
  """
    |spark-submit \
    |--class sparkmllib.tree \
    |--master local \
    |/var/lib/hadoop-hdfs/spark-work/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    //1 构建spark对象
    val conf = new SparkConf().setAppName("LinearRegressionDemo")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据 格式为LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc,"hdfs://nameservice1/sparkmllib/data/sample_libsvm_data.txt")
    //Split the data into training and sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7,0.3))
    val (trainingData,testData) = (splits(0),splits(1))

    //新建决策树
    val numClasses = 2  //二元分类
    val categoricalFeaturesInfo = Map[Int, Int]()  //离散特征信息（为空）
    val impurity = "gini"  //信息增益的方法  其中方差只能用在回归中
    val maxDepth = 5   //最大深度
    val maxBins = 32   //每个特征最大能划分32个bins

    val model = DecisionTree.trainClassifier(trainingData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)

    //打印决策树模型全部
    model.toDebugString

    //误差计算
    val labelAndPreds = testData.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })

    val print_predict = labelAndPreds.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1){
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val testErr = labelAndPreds.filter(r=>r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    //保存模型
    val ModelPath = "hdfs://nameservice1/sparkmllib/model/Decision_Tree_Model"
    model.save(sc, ModelPath)
    val saveModel = DecisionTreeModel.load(sc, ModelPath)
    println("load model:\n" + saveModel.toDebugString)


  }
}
