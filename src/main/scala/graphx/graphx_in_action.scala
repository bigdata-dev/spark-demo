package graphx

import java.io.PrintWriter

/**
  * Created by tonye0115 on 2017/8/18.
  */
object raphx_in_action {

  import org.apache.spark._
  val conf = new SparkConf().setAppName("")
  val sc = new SparkContext(conf)


  sc.setLogLevel("ERROR")
  import org.apache.spark.graphx._
  //http://snap.stanford.edu/data/cit-HepTh.txt.gz
  val graph = GraphLoader.edgeListFile(sc, "hdfs://nameservice1/graphx/cit-HepTh.txt")

  //输出被引用最多的一个论文ID inDegrees查询入度
  graph.inDegrees.reduce((a,b) => if(a._2 > b._2) a else b)
  graph.vertices.take(10)
  graph.edges.take(10)

  //PageRank算法 参数0.001是平衡速度和准确度一个容忍度值
  val graph_new = graph.pageRank(0.1)
  //每个顶点的属性值就是PageRank值
  val v = graph_new.vertices
  v.take(10)

  //reduce归并函数，找出PageRank值的最高点的顶点
  v.reduce((a,b) => if(a._2 > b._2) a else b)


  //构建图
  sc.setLogLevel("ERROR")
  import org.apache.spark.graphx._
  val myVertices = sc.makeRDD(Array((1L,"Ann"), (2L,"Bill"), (3l,"Charles"), (4l,"Diane"),(5L,"Went to gym this morning")))
  val myEdges = sc.makeRDD(Array(
    Edge(1L, 2L, "is-friends-with"),
    Edge(2L, 3L, "is-friends-with"),
    Edge(3L, 4L, "is-friends-with"),
    Edge(4L, 5L, "Likes-status"),
    Edge(3L, 5L, "Wrote-status")))
  val myGraph = Graph(myVertices, myEdges)
  myGraph.vertices.collect()
  myGraph.triplets.collect()
  myGraph.edges.collect()


  myGraph.mapTriplets(t=>(t.attr,t.attr=="is-friends-with"&&t.srcAttr.toLowerCase.contains("a"))).triplets.collect()

  myGraph.mapTriplets((t=>(t.attr,t.attr=="is-friends-with"&&t.srcAttr.toLowerCase.contains("a")))
    :(EdgeTriplet[String,String]=>Tuple2[String,Boolean])).triplets.collect()

  //统计每个顶点的出度
  //aggregateMessage两个参数提供了sendMsg转换和mergeMsg聚合的能力
  //sendToSrc 将Msg类型的消息发送给源顶点 在边上将包含整数1的消息发送到源顶点
  myGraph.aggregateMessages[Int](_.sendToSrc(1),_+_).collect()

  //统计每个顶点的入度
  myGraph.aggregateMessages[Int](_.sendToDst(1),_+_).collect()

  //join操作 匹配VertexId与顶点数据
  myGraph.aggregateMessages[Int](_.sendToSrc(1), _+_).join(myGraph.vertices).collect

  //使用map()和swap整理输出
  myGraph.aggregateMessages[Int](_.sendToSrc(1),_+_).join(myGraph.vertices).map(_._2.swap).collect()

  //使用rightOuterJoin代替join找回丢失的顶点
  myGraph.aggregateMessages[Int](_.sendToSrc(1),_+_).rightOuterJoin(myGraph.vertices).map(_._2.swap).collect()

  //Option[]的getOrElse()方法可用于整理rightOuterJoin()的输出
  myGraph.aggregateMessages[Int](_.sendToSrc(1),_+_).rightOuterJoin(myGraph.vertices).map(
    x=>(x._2._2,x._2._1.getOrElse(0))).collect()

  //每个顶点标记离他最远的顶点的距离
  //定义sendMsg函数 发到目标的顶点的累加计数器
  def sendMsg(ec:EdgeContext[Int,String,Int]):Unit = {
    ec.sendToDst(ec.srcAttr+1)
  }

  //定义mergeMsg函数 这个函数会在所有消息传递给顶点后被重复调用
  //消息进过合并后，最终得出结果为包含最大距离值的顶点
  def mergeMsg(a:Int,b:Int):Int = {
    math.max(a,b)
  }

  //定义递归辅助函数
  def propagateEdgeCount(g:Graph[Int,String]):Graph[Int,String] = {
    //初始化 图
    //Array(((1,0),(2,0),is-friends-with), ((2,0),(3,0),is-friends-with), ((3,0),(4,0),is-friends-with), ((3,0),(5,0),Wrote-status), ((4,0),(5,0),Likes-status))

    //生成新的顶底集
    //第一次 (4,1)(2,1)(3,1)(5,1)
    //第二次 (4,2)(2,1)(3,2)(5,2)
    //第三次 (4,3)(2,1)(3,2)(5,3)
    //第四次 (4,3)(2,1)(3,2)(5,4)
    //第五次 (4,3)(2,1)(3,2)(5,4)
    val verts = g.aggregateMessages[Int](sendMsg,mergeMsg)
    verts.collect().foreach(print)
    println()

    //生成一个更新后的包含新的信息的图 会补上丢失的顶点
    //第一次
    //g2.triplets.collect()
    //res7: Array[org.apache.spark.graphx.EdgeTriplet[Int,String]] = Array(((1,0),(2,1),is-friends-with), ((2,1),(3,1),is-friends-with), ((3,1),(4,1),is-friends-with), ((3,1),(5,1),Wrote-status), ((4,1),(5,1),Likes-status))
    //第二次
    //res5: Array[org.apache.spark.graphx.EdgeTriplet[Int,String]] = Array(((1,0),(2,1),is-friends-with), ((2,1),(3,2),is-friends-with), ((3,2),(4,2),is-friends-with), ((3,2),(5,2),Wrote-status), ((4,2),(5,2),Likes-status))
    val g2 = Graph(verts,g.edges)

    //将两组顶点连接在一起来看看更新的图
    //第一次
    //scala>   g2.vertices.join(initialGraph.vertices).collect()
    //res6: Array[(org.apache.spark.graphx.VertexId, (Int, Int))] = Array((4,(1,0)), (2,(1,0)), (1,(0,0)), (3,(1,0)), (5,(1,0)))
    //第二次
    //res6: Array[(org.apache.spark.graphx.VertexId, (Int, Int))] = Array((4,(2,1)), (2,(1,1)), (1,(0,0)), (3,(2,1)), (5,(2,1)))
    val check = g2.vertices.join(g.vertices).map(
      x=>x._2._1 - x._2._2
    ).reduce(_ + _)
    if (check>0)
      propagateEdgeCount(g2)
    else
      g
  }


  //initialGraph.triplets.collect()
  //res9: Array[org.apache.spark.graphx.EdgeTriplet[Int,String]] = Array(((1,0),(2,0),is-friends-with), ((2,0),(3,0),is-friends-with), ((3,0),(4,0),is-friends-with), ((3,0),(5,0),Wrote-status), ((4,0),(5,0),Likes-status))
  val initialGraph = myGraph.mapVertices((_,_) => 0)
  initialGraph.triplets.collect()
  propagateEdgeCount(initialGraph).vertices.collect()

//辅助测试
//  val verts = initialGraph.aggregateMessages[Int](sendMsg,mergeMsg)
//  verts.collect()
//
//  //生成一个更新后的包含新的信息的图
//  val g2 = Graph(verts,initialGraph.edges)
//  g2.vertices.collect()
//  g2.edges.collect()
//  g2.triplets.collect()
//
//  val init2 = g2.aggregateMessages[Int](sendMsg,mergeMsg)
//  init2.collect()
//
//  //生成一个更新后的包含新的信息的图
//  val g2_2 = Graph(init2,g2.edges)
//  g2_2.triplets.collect()
//
//
//
//  g2.vertices.join(initialGraph.vertices).collect()
//  g2_2.vertices.join(g2.vertices).collect()
//
//  val init3 = g2_2.aggregateMessages[Int](sendMsg,mergeMsg)
//  init3.collect()


  //写到hdfs
  myGraph.vertices.saveAsObjectFile("hdfs://nameservice1/graphx/myGraphVertices")
  myGraph.edges.saveAsObjectFile("hdfs://nameservice1/graphx/myGraphEdges")

  //读图
  val myGraph2 = Graph(sc.objectFile[Tuple2[VertexId, String]]("hdfs://nameservice1/graphx/myGraphVertices"),
    sc.objectFile[Edge[String]]("hdfs://nameservice1/graphx/myGraphEdges"))
  myGraph2.triplets.collect()



  //写到hdfs
  myGraph.vertices.coalesce(1, true).saveAsObjectFile("hdfs://nameservice1/graphx/myGraphVertices1")
  myGraph.edges.coalesce(1, true).saveAsObjectFile("hdfs://nameservice1/graphx/myGraphEdges1")

  //读图
  val myGraph3 = Graph(sc.objectFile[Tuple2[VertexId, String]]("hdfs://nameservice1/graphx/myGraphVertices1"),
    sc.objectFile[Edge[String]]("hdfs://nameservice1/graphx/myGraphEdges1"))
  myGraph3.triplets.collect()

  //合并文件的正确做法
  //hadoop fs -getmerge hdfs://nameservice1/graphx/myGraphVertices  hdfs://nameservice1/graphx/myGraphVerticesFile
  //hadoop fs -getmerge hdfs://nameservice1/graphx/myGraphEdges  myGraphEdgesFile

  //在REPL中启用jackon-module-scala命令行
  //wget http://repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-scala_2.10/2.4.4/jackson-module-scala_2.10-2.4.4.jar
  //wget http://repo1.maven.org/maven2/com/google/guava/guava/14.0.1/guava-14.0.1.jar

  //spark-shell --jars  jackson-module-scala_2.10-2.4.4.jar,guava-14.0.1.jar

  //序列化为json的简单方法
  myGraph.vertices.map(x=>{
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val writer = new java.io.StringWriter()
    mapper.writeValue(writer, x)
    writer.toString
  }).coalesce(1, true).saveAsTextFile("hdfs://nameservice1/graphx/myGraphVertices_json")


  myGraph.triplets.map(x=>{
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val writer = new java.io.StringWriter()
    mapper.writeValue(writer, x)
    writer.toString
  }).coalesce(1, true).saveAsTextFile("hdfs://nameservice1/graphx/myGraphTriplets_json")


  //性能更佳的json序列化和反序列化
  import com.fasterxml.jackson.core.`type`.TypeReference
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  myGraph.vertices.mapPartitions(vertices=>{
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val writer = new java.io.StringWriter()
    vertices.map(v => {
      writer.getBuffer.setLength(0)
      mapper.writeValue(writer, v)
      writer.toString
    })
  }).coalesce(1, true).saveAsTextFile("hdfs://nameservice1/graphx/myGraphVertices_json_mapPartitions")

  myGraph.edges.mapPartitions(vertices=>{
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
    val writer = new java.io.StringWriter()
    vertices.map(v => {
      writer.getBuffer.setLength(0)
      mapper.writeValue(writer, v)
      writer.toString
    })
  }).coalesce(1, true).saveAsTextFile("hdfs://nameservice1/graphx/myGraphEdges_json_mapPartitions")

  //反序列图'
//  sc.setLogLevel("ERROR")
//  import org.apache.spark.graphx._
//  val myGraph2_new = Graph(
//    sc.textFile("hdfs://nameservice1/graphx/myGraphVertices_json_mapPartitions").mapPartitions(vertices=>{
//      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
//      mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
//      vertices.map(v=>{
//        val r = mapper.readValue[Tuple2[Integer,String]](v, new com.fasterxml.jackson.core.`type`.TypeReference[Tuple2[Integer,String]] {})
//        (r._1.toLong,r._2)
//      })
//    }),
//    sc.textFile("hdfs://nameservice1/graphx/myGraphEdges_json_mapPartitions").mapPartitions(edges=>{
//      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
//      mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
//      edges.map(e=> mapper.readValue[Edge[String]](e, new com.fasterxml.jackson.core.`type`.TypeReference[Edge[String]]{}))
//    })
//  )
//
//  myGraph2_new.triplets.collect()

  //导出gexf格式
  //使用:paste进入粘贴模式 ctrl+D退出
  def toGexf[VD,ED](g:Graph[VD,ED]) = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "    <nodes>\n" + g.vertices.map(v => "      <node id=\"" +
    v._1 + "\" label=\"" + v._2 + "\" />\n")
    .collect.mkString + "    </nodes>\n" + "    <edges>\n" +
    g.edges.map(e => "      <edge source=\"" + e.srcId + "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
      "\" />\n").collect.mkString + "    </edges>\n" + "  </graph>\n" + "</gexf>"

  val pw = new java.io.PrintWriter("myGraph.gexf")
  pw.write(toGexf(myGraph))
  pw.close()


  //生成网格图
//  val pw = new java.io.PrintWriter("gridGraph.gexf")
//  pw.write(toGexf(org.apache.spark.graphx.util.GraphGenerators.gridGraph(sc, 4, 4)))
//  pw.close


//  //使用Pregel来找到距离最远的顶点
//  val g = Pregel(myGraph.mapVertices((vid,vd) => 0),0,activeDirection = EdgeDirection.Out)
//  ((id:VertexId, vd:Int,a:Int) => math.max(vd, a),
//    )

}
