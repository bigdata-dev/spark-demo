package sparkmllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tonye0115 on 2017/3/31.
  */
object LinearRegressionDemo {
  """
    |spark-submit \
    |--class sparkmllib.LinearRegressionWithSGDDemo \
    |--master local \
    |/var/lib/hadoop-hdfs/spark-jar/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinearRegressionDemo")
    val sc = new SparkContext(conf)

    //Load and parse the data
    val data = sc.textFile("hdfs://nameservice1/sparkmllib/data/testSet.txt")
    val examples = data.map(line =>{
      val parts =  line.split("\t")
      val labeledData = parts(parts.length-1).toDouble
      val b = ArrayBuffer[String]()
      b ++=parts
      b.trimEnd(1)
      val featuresData = b.toArray
      LabeledPoint(labeledData,Vectors.dense(featuresData.map(_.toDouble)))
    }).cache()

    val numExamples = examples.count()

    //Building the model
    val numIterations = 100  //循环次数
    val stepSize = 1    //步长
    val minBatchFraction = 0.4  //随机抽样比例
    val model = LinearRegressionWithSGD.train(examples, numIterations,stepSize,minBatchFraction)
    model.weights  //权重
    model.intercept //偏执

    //对样本进行预测
    val prediction = model.predict(examples.map(_.features))
    val predictionAndLabel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLabel.take(20)
    println("predication" + "\t" + "label")
    for(i <- 0 to print_predict.length -1){
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    //计算测试误差
    val loss = predictionAndLabel.map({
      case (p, l) =>
        val err = p -l
        err * err
    }).reduce(_ + _)

    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMST = $rmse.")

    //模型保存
    val ModelPath = "hdfs://nameservice1/sparkmllib/mode/LinearRegressionModel"
    model.save(sc, ModelPath)
    val sameModel = LinearRegressionModel.load(sc, ModelPath)






  }

}
