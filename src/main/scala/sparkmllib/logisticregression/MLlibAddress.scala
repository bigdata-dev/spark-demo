package sparkmllib.logisticregression

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by tonye0115 on 2016/12/13.
  */
object MLlibAddress {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName(s"MLlib example").setMaster("local")
    val sc = new SparkContext(conf)
    val geo: RDD[String] = sc.textFile("src\\main\\scala\\sparkmllib\\logisticregression\\file\\geoinfo-0000.txt")

    val map: RDD[(String, String)] = geo.map(line => {
      val split: Array[String] = line.split("\t")
      val temp = ToAnalysis.parse(split(4))
      val word = for(i<-Range(0,temp.size())) yield temp.get(i).getName
      (split(10),split(1)+"\t"+split(2)+"\t"+split(3)+"\t"+word.mkString("\t"))
    })

    val tf: HashingTF = new HashingTF(numFeatures = 100)

    val trainingData: RDD[LabeledPoint] = map.map(rdd => {
      LabeledPoint(rdd._1.toInt, tf.transform(rdd._2.split("\t")))
    })


    map.foreach(println)


    trainingData.cache()

    val lrLearner = new LogisticRegressionWithSGD()

    val model = lrLearner.run(trainingData)


    val temp = ToAnalysis.parse("观珠镇观珠镇银珠酒店深茂项目部")
    val word = for(i<-Range(0,temp.size())) yield temp.get(i).getName

    print("word:",word.mkString("\t"))

    val posTestExample = tf.transform("广东省\t茂名市\t电白区\t观珠镇观珠镇银珠酒店深茂项目部\t"+word.mkString("\t").split("\t"))

    println(s"Prediction for negative test example: ${model.predict(posTestExample)}")
    sc.stop()
  }

}
