import org.apache.spark.SparkConf

/**
  * Created by tangfei on 2016/10/28.
  */
object WebPagePopularityValueCalculator {
  def main(args: Array[String]) {
    if(args.length<2){
      println("Usage:WebPagePopularityValueCalculator zk1 zk2")
      System.exit(1)
    }

    new SparkConf()
  }

}
