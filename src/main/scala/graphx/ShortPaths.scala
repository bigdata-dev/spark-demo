package graphx

import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tonye0115 on 2017/8/17.
  */
object ShortPaths {

  def main(args:Array[String]):Unit={
    import org.apache.spark._
    val conf = new SparkConf().setAppName("ShortPaths").setMaster("local[4]")
    val sc = new SparkContext(conf)


    sc.setLogLevel("INFO")

    import org.apache.spark._
    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.graphx.lib.ShortestPaths

    val shortPaths = Set(
      (1,Map(1->0,4->2)),(2,Map(1->1,4->2)),(3,Map(1->2,4->1)),
      (4,Map(1->2,4->0)),(5,Map(1->1,4->2)),(6,Map(1->3,4->1))
    )

    // 构造无向图的边序列
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
      case e => Seq(e, e.swap)
    }

    // 构造无向图
    val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
    val graph = Graph.fromEdgeTuples(edges, 1)


    println("\ngraph edges")
    println("edges:")
    graph.edges.collect.foreach(println)
    println("vertices:");
    graph.vertices.collect.foreach(println)
    println("triplets:");
    graph.triplets.collect.foreach(println)
    println();



    // 要求最短路径的点集合
    val landmarks = Seq(1, 2, 3,4,5,6).map(_.toLong)

    // 计算最短路径
    val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
      case (v, spMap) => (v, spMap.mapValues(i => i))
    }









  }

}
