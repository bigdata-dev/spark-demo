package graphx

/**
  * Created by tonye0115 on 2017/7/21.
  */
object demo1 {
  import org.apache.spark._
  val conf = new SparkConf().setAppName("")
  val sc = new SparkContext(conf)


  sc.setLogLevel("INFO")

  import org.apache.spark._
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  // 构建user vertices
  val user:RDD[(VertexId,(String,String))]= sc.parallelize(Array((3L,("rxin","student")),
    (7L,("jgonzal","postdoc")),
    (5L,("franklin","prof")),
    (2L,("istoica","prof"))))

  // 构建relationship  edge
  val relationship:RDD[Edge[String]] = sc.parallelize(Array(Edge(3L,7L,"collab"),
    Edge(5L,3L,"advisor"),
    Edge(2L,5L,"colleague"),
    Edge(5L,7L,"pi")))

  //放入defaultUser
  val defaultUser = ("John Doe","Missing")

  //构造图
  val graph = Graph(user, relationship, defaultUser)

  //查询职业occupation为postdoc的顶点个数
  graph.vertices.filter{ case(id, (name,post)) =>post == "postdoc"}.count()

  //查看EdgeTriplet
  graph.triplets.collect()

  //查看vertices.collect
  graph.vertices.collect()

  //加载hdfs中的web-Google.txt构建graph yarn-client方法 默认2个分区
  val graph_webgoogle = GraphLoader.edgeListFile(sc, "hdfs://nameservice1/graph/web-Google.txt")


  //加载hdfs中的web-Google.txt构建graph 指定4个分区
  val graph_webgoogle1 = GraphLoader.edgeListFile(sc, "hdfs://nameservice1/graph/web-Google.txt", numEdgePartitions=4)

  ///////////////////// graph操作之 Property Operators
  //查询顶点个数
  graph_webgoogle1.vertices.count()

  //查询边数量
  graph_webgoogle1.edges.count()

  //查看graph实例中10个元素的具体值 默认都是1
  graph_webgoogle1.vertices.take(10)
  graph_webgoogle1.triplets.take(10)

  //将每个顶点的元素值都变为3
  val tmp = graph_webgoogle1.mapVertices((id, attr) => attr.toInt*3)

  //查看每个顶点的属性变为3
  tmp.vertices.take(10)

  //对边进行操作
  graph_webgoogle1.edges.take(10)
  val tmp1 = graph_webgoogle1.mapEdges(e => e.attr.toInt * 2)
  tmp1.edges.take(10)

  //对 triplets 操作 将每个元素的edge属性值设置为源顶点属性值得2倍加上目标属性值的3倍
  val tmp2 = graph_webgoogle1.mapTriplets(et => et.srcAttr.toInt * 2 + et.dstAttr.toInt * 3)
  tmp2.triplets.take(10)

  ///////////////////// graph操作之 structural Operators
  //对顶点过滤子图构建
  val subgraph = graph_webgoogle1.subgraph(epred = e => e.srcId>e.dstId, vpred = (id,_) => id > 500000)
  subgraph.vertices.count()
  subgraph.edges.count()


  ///////////////////// graph操作之 Computing Degree
  val tmp_10 = graph_webgoogle1.inDegrees
  tmp_10.take(10)

  val tmp_11 = graph_webgoogle1.outDegrees
  tmp_11.take(10)
  //比较Degree最大
  def max(a: (VertexId,Int), b:(VertexId,Int)):(VertexId,Int) = if (a._2 > b._2) a else b
  graph_webgoogle1.degrees.reduce(max)

  ///////////////////// graph操作之 join Operators
  //把所有顶点属性变为0
  val rawGraph = graph_webgoogle1.mapVertices((id, attr) =>0)
  rawGraph.vertices.take(10)

  //找到所有outDegree的顶点的集合
  val outDeg = rawGraph.outDegrees

  //joinVertices操作
  val tmp12 = rawGraph.joinVertices[Int](outDeg)((_, _, optDeg) => optDeg)
  tmp12.vertices.take(10)

  //outJoinVerties操作
  val tmp13 = rawGraph.outerJoinVertices[Int,Int](outDeg)((_,_,optDeg) => optDeg.getOrElse(-1))
  tmp13.vertices.take(20)

  ///////////////////// graph操作之 map reduce Triplets
  //随机生成一张图 年龄图
  import org.apache.spark.graphx.util.GraphGenerators
  val graph_random:Graph[Double, Int] = GraphGenerators.
    logNormalGraph(sc, numVertices =  100).mapVertices((id, _) => id.toDouble)
  graph_random.vertices.take(10).mkString("\n")
  graph_random.edges.take(10).mkString("\n")

  //把图的顶点ID当做用户年龄，需要计算出所有比这个用户年龄大的用户的个数以及比这个用户年龄大的平均年龄
  val oldFollowers: VertexRDD[(Int, Double)] = graph_random.mapReduceTriplets[(Int, Double)](
    triplet => {
      if (triplet.srcAttr > triplet.dstAttr){
        Iterator((triplet.dstId, (1, triplet.srcAttr)))
      }else{
        Iterator.empty
      }
    },
    (a, b) => (a._1 + b._1, a._2 + b._2)
  )

  val avgAgeOfOlderFollowers:VertexRDD[String] =
    oldFollowers.mapValues((id, value) => value  match {case (count, totalAge) => totalAge/count+"_"+count})
  avgAgeOfOlderFollowers.collect().foreach(println(_))

  ///////////////////// graph操作之 Pregel API
  //计算最短路径

  sc.setLogLevel("INFO")

  import org.apache.spark._
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  //加载hdfs中的web-Google.txt构建graph 指定4个分区
  val graph_webgoogle2 = GraphLoader.edgeListFile(sc, "hdfs://nameservice1/graph/web-Google.txt", numEdgePartitions=2)


  val sourceId : VertexId = 0
  val g = graph_webgoogle2.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
  val tmp110 = g.mapTriplets(triplet => { println("==triplet.srcId:"+triplet.srcId+
    ", triplet.srcAttr:"+triplet.srcAttr+",  triplet.attr:"+triplet.attr+", triplet.dstId:"+
    triplet.dstId+",  triplet.dstAttr:"+triplet.dstAttr)})
  tmp110.triplets.take(10)

  val sssp = g.pregel(Double.PositiveInfinity)(
    (id,  dist, newDist) => math.min(dist, newDist),
    triplet => {
      if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
        println("==triplet.srcId:"+triplet.srcId+
          ", triplet.srcAttr:"+triplet.srcAttr+",  triplet.attr:"+triplet.attr+", triplet.dstId:"+
          triplet.dstId+",  triplet.dstAttr:"+triplet.dstAttr)
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      }else{
        Iterator.empty
      }
    },
    (a, b) => math.min(a, b)
  )

  sssp.vertices.take(10).mkString("\n")


  //pageRank
  val rank = graph_webgoogle2.pageRank(0.01).vertices









}

