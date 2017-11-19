package graphx



/**
  * Created by tonye0115 on 2017/8/17.
  */
object Pregel {

  def main(args: Array[String]) {
    import org.apache.spark._
    val conf = new SparkConf().setAppName("ShortPaths").setMaster("local[4]")
    val sc = new SparkContext(conf)


    sc.setLogLevel("INFO")

    import org.apache.spark._
    import org.apache.spark.graphx._
    import org.apache.spark.rdd.RDD
    import org.apache.spark.graphx.lib.ShortestPaths
    import org.apache.spark.graphx.util.GraphGenerators

    // A graph with edge attributes containing distances
    //初始化一个随机图，节点的度符合对数正态分布,边属性初始化为1
    val graph: Graph[Long, Double] = GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
    graph.edges.collect.foreach(println)
    graph.triplets.collect.foreach(println)
    graph.vertices.collect.foreach(println)
    """
      |(4,7)
      |(0,5)
      |(1,1)
      |(6,8)
      |(3,9)
      |(7,8)
      |(9,9)
      |(8,1)
      |(5,2)
      |(2,6)
    """.stripMargin


    val sourceId: VertexId = 4 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    //初始化各节点到原点的距离
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    initialGraph.edges.collect.foreach(println)
    initialGraph.vertices.collect.foreach(println)
    """
      |(4,0.0)
      |(0,Infinity)
      |(1,Infinity)
      |(6,Infinity)
      |(3,Infinity)
      |(7,Infinity)
      |(9,Infinity)
      |(8,Infinity)
      |(5,Infinity)
      |(2,Infinity)
    """.stripMargin
    initialGraph.triplets.collect.foreach(println)
    """
      |((0,Infinity),(0,Infinity),1.0)
      |((0,Infinity),(1,Infinity),1.0)
      |((0,Infinity),(3,Infinity),1.0)
      |((0,Infinity),(8,Infinity),1.0)
      |((0,Infinity),(9,Infinity),1.0)
      |((1,Infinity),(3,Infinity),1.0)
      |((2,Infinity),(0,Infinity),1.0)
      |((2,Infinity),(0,Infinity),1.0)
      |((2,Infinity),(1,Infinity),1.0)
      |((2,Infinity),(6,Infinity),1.0)
      |((2,Infinity),(7,Infinity),1.0)
      |((2,Infinity),(9,Infinity),1.0)
      |((3,Infinity),(0,Infinity),1.0)
      |((3,Infinity),(1,Infinity),1.0)
      |((3,Infinity),(4,0.0),1.0)
      |((3,Infinity),(4,0.0),1.0)
      |((3,Infinity),(5,Infinity),1.0)
      |((3,Infinity),(6,Infinity),1.0)
      |((3,Infinity),(6,Infinity),1.0)
      |((3,Infinity),(8,Infinity),1.0)
      |((3,Infinity),(8,Infinity),1.0)
      |((4,0.0),(0,Infinity),1.0)
      |((4,0.0),(1,Infinity),1.0)
      |((4,0.0),(4,0.0),1.0)
      |((4,0.0),(5,Infinity),1.0)
      |((4,0.0),(6,Infinity),1.0)
      |((4,0.0),(7,Infinity),1.0)
      |((4,0.0),(7,Infinity),1.0)
      |((5,Infinity),(4,0.0),1.0)
      |((5,Infinity),(5,Infinity),1.0)
      |((6,Infinity),(0,Infinity),1.0)
      |((6,Infinity),(0,Infinity),1.0)
      |((6,Infinity),(2,Infinity),1.0)
      |((6,Infinity),(2,Infinity),1.0)
      |((6,Infinity),(2,Infinity),1.0)
      |((6,Infinity),(4,0.0),1.0)
      |((6,Infinity),(6,Infinity),1.0)
      |((6,Infinity),(9,Infinity),1.0)
      |((7,Infinity),(2,Infinity),1.0)
      |((7,Infinity),(3,Infinity),1.0)
      |((7,Infinity),(6,Infinity),1.0)
      |((7,Infinity),(6,Infinity),1.0)
      |((7,Infinity),(6,Infinity),1.0)
      |((7,Infinity),(6,Infinity),1.0)
      |((7,Infinity),(8,Infinity),1.0)
      |((7,Infinity),(9,Infinity),1.0)
      |((8,Infinity),(4,0.0),1.0)
      |((9,Infinity),(0,Infinity),1.0)
      |((9,Infinity),(1,Infinity),1.0)
      |((9,Infinity),(1,Infinity),1.0)
      |((9,Infinity),(2,Infinity),1.0)
      |((9,Infinity),(2,Infinity),1.0)
      |((9,Infinity),(2,Infinity),1.0)
      |((9,Infinity),(3,Infinity),1.0)
      |((9,Infinity),(7,Infinity),1.0)
      |((9,Infinity),(8,Infinity),1.0)
    """.stripMargin


    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program，节点处理消息的函数，dist为原节点属性（Double），newDist为消息类型（Double）
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message，发送消息函数，返回结果为（目标节点id，消息（即最短距离））
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          println(triplet.srcId+"-"+"-"+triplet.dstId)
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      //Merge Message，对消息进行合并的操作，类似于Hadoop中的combiner
      (a, b) => math.min(a, b)
    )

    """
      |4--0
      |4--1
      |4--5
      |4--6
      |4--7
      |4--7
      |0--3
      |0--8
      |0--9
      |1--3
      |6--2
      |6--2
      |6--2
      |6--9
      |7--2
      |7--3
      |7--8
      |7--9
    """.stripMargin

    println(sssp.vertices.collect.mkString("\n"))

    """
      |(4,0.0)
      |(0,1.0)
      |(1,1.0)
      |(6,1.0)
      |(3,2.0)
      |(7,1.0)
      |(9,2.0)
      |(8,2.0)
      |(5,1.0)
      |(2,2.0)
    """.stripMargin

    println(sssp.triplets.collect.mkString("\n"))
    """
      |((0,1.0),(0,1.0),1.0)
      |((0,1.0),(1,1.0),1.0)
      |((0,1.0),(3,2.0),1.0)
      |((0,1.0),(8,2.0),1.0)
      |((0,1.0),(9,2.0),1.0)
      |((1,1.0),(3,2.0),1.0)
      |((2,2.0),(0,1.0),1.0)
      |((2,2.0),(0,1.0),1.0)
      |((2,2.0),(1,1.0),1.0)
      |((2,2.0),(6,1.0),1.0)
      |((2,2.0),(7,1.0),1.0)
      |((2,2.0),(9,2.0),1.0)
      |((3,2.0),(0,1.0),1.0)
      |((3,2.0),(1,1.0),1.0)
      |((3,2.0),(4,0.0),1.0)
      |((3,2.0),(4,0.0),1.0)
      |((3,2.0),(5,1.0),1.0)
      |((3,2.0),(6,1.0),1.0)
      |((3,2.0),(6,1.0),1.0)
      |((3,2.0),(8,2.0),1.0)
      |((3,2.0),(8,2.0),1.0)
      |((4,0.0),(0,1.0),1.0)
      |((4,0.0),(1,1.0),1.0)
      |((4,0.0),(4,0.0),1.0)
      |((4,0.0),(5,1.0),1.0)
      |((4,0.0),(6,1.0),1.0)
      |((4,0.0),(7,1.0),1.0)
      |((4,0.0),(7,1.0),1.0)
      |((5,1.0),(4,0.0),1.0)
      |((5,1.0),(5,1.0),1.0)
      |((6,1.0),(0,1.0),1.0)
      |((6,1.0),(0,1.0),1.0)
      |((6,1.0),(2,2.0),1.0)
      |((6,1.0),(2,2.0),1.0)
      |((6,1.0),(2,2.0),1.0)
      |((6,1.0),(4,0.0),1.0)
      |((6,1.0),(6,1.0),1.0)
      |((6,1.0),(9,2.0),1.0)
      |((7,1.0),(2,2.0),1.0)
      |((7,1.0),(3,2.0),1.0)
      |((7,1.0),(6,1.0),1.0)
      |((7,1.0),(6,1.0),1.0)
      |((7,1.0),(6,1.0),1.0)
      |((7,1.0),(6,1.0),1.0)
      |((7,1.0),(8,2.0),1.0)
      |((7,1.0),(9,2.0),1.0)
      |((8,2.0),(4,0.0),1.0)
      |((9,2.0),(0,1.0),1.0)
      |((9,2.0),(1,1.0),1.0)
      |((9,2.0),(1,1.0),1.0)
      |((9,2.0),(2,2.0),1.0)
      |((9,2.0),(2,2.0),1.0)
      |((9,2.0),(2,2.0),1.0)
      |((9,2.0),(3,2.0),1.0)
      |((9,2.0),(7,1.0),1.0)
      |((9,2.0),(8,2.0),1.0)
    """.stripMargin

  }

}
