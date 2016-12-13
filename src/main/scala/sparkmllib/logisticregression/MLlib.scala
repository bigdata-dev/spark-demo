package sparkmllib.logisticregression

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tonye0115 on 2016/12/13.
  */
object MLlib {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName(s"MLlib example").setMaster("local")
    val sc = new SparkContext(conf)

    val spam: RDD[String] = sc.textFile("src\\main\\scala\\sparkmllib\\logisticregression\\file\\spam.txt")
    val ham: RDD[String] = sc.textFile("src\\main\\scala\\sparkmllib\\logisticregression\\file\\ham.txt")

    val tf: HashingTF = new HashingTF(numFeatures = 100)

    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples ++ negativeExamples

    trainingData.cache()

    val lrLearner = new LogisticRegressionWithSGD()

    val model = lrLearner.run(trainingData)


    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))

    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))

    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")
    sc.stop()

  }
}
