package sparkneo4j

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.neo4j.spark.{Neo4jConfig, Neo4jGraph}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tonye0115 on 2017/11/13.
  */
object SparkNeo4j {
  """
      |spark-submit \
      |--master yarn \
      |--deploy-mode client \
      |--num-executors 20 \
      |--executor-memory 5G \
      |--conf spark.neo4j.bolt.url=bolt://0.0.0.0:7687 \
      |--conf spark.neo4j.bolt.user=neo4j \
      |--conf spark.neo4j.bolt.password=***  \
      |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-spark-connector-1.0.0-RC1.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/fastjson-1.2.40.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/graphframes-0.1.0-spark1.6.jar \
      |--class sparkneo4j.SparkNeo4j \
      |/home/tonye/spark-work/spark-demo.jar \
      |20170915
      |
      |spark-submit \
      |--master local \
      |--executor-memory 2G \
      |--conf spark.neo4j.bolt.url=bolt://0.0.0.0:7687 \
      |--conf spark.neo4j.bolt.user=neo4j \
      |--conf spark.neo4j.bolt.password=***  \
      |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-spark-connector-1.0.0-RC1.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/fastjson-1.2.40.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/graphframes-0.1.0-spark1.6.jar \
      |--class sparkneo4j.SparkNeo4j \
      |/home/tonye/spark-work/spark-demo.jar \
      |20170915
    |
    |
  """.stripMargin
  def main(args: Array[String]) {

    val Array(ds) = args
    val conf = new SparkConf().setAppName("SparkNeo4j")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
//    val neo4jConfig = new Neo4jConfig(sc.getConf.get("spark.neo4j.bolt.url"),
//      sc.getConf.get("spark.neo4j.bolt.user"),
//      Option(sc.getConf.get("spark.neo4j.bolt.password")))


        val resultDF = hiveContext.sql(
          """
            |SELECT t.bill_code,
            |concat_ws('|', collect_list(t.scan_site_id)) as scan_site_id,
            |concat_ws('|', collect_list(t.fullname)) as fullname,
            |concat_ws('|', collect_list(t.categorycode)) as categorycode,
            |concat_ws('|', collect_list(t.scan_date)) as scan_date,
            |concat_ws('|', collect_list(t.come_date)) as come_date,
            |concat_ws('|', collect_list(t.send_date)) as send_date,
            |concat_ws('|', collect_list(cast(t.send_num as string))) as send_num
            |FROM (select t1.bill_code, t1.scan_site_id,t2.fullname,t2.categorycode,t1.scan_date,nvl(t1.come_date,'0') come_date,nvl(t1.send_date,'0') send_date,t1.send_num from tmp.tmp_bill3 t1 join dim.baseorganize t2 on t1.scan_site_id=t2.id
            | order by t1.bill_code,t1.send_num) t
            |GROUP BY t.bill_code
          """.stripMargin
        )

//    val resultDF = hiveContext.sql(
//      """
//          |select bill_code,scan_site_id,fullname,categorycode,scan_date,come_date,send_date from tmp.tmp_vrp1 limit 1
//        """.stripMargin
//    )


   // resultDF.collect().foreach(println)

    resultDF.foreachPartition(it=>{
      it.foreach(row =>{
        val bill_code = row.getAs[String]("bill_code")
        val scan_site_ids = row.getAs[String]("scan_site_id").split('|')
        val scan_site_names = row.getAs[String]("fullname").split('|')
        val category_codes = row.getAs[String]("categorycode").split('|')
        val scan_dates = row.getAs[String]("scan_date").split('|')
        val send_dates = row.getAs[String]("send_date").split('|')
        val come_dates = row.getAs[String]("come_date").split('|')
        val nodes = ArrayBuffer[Map[String,String]]()
        val node_relations = ArrayBuffer[Map[String,String]]()
        for (i <- 0 to scan_site_ids.length - 1) {
          var node_map = Map[String, String]()
          node_map += ("node_id" -> scan_site_ids(i))
          node_map += ("node_name" -> scan_site_names(i))
          node_map += ("ategory_code" -> category_codes(i))
          nodes += node_map

          var relation_map = Map[String, String]()
          if (i != scan_site_ids.length - 1) {
            relation_map += ("bill_code" -> bill_code)
            relation_map += ("ds" -> ds)
            relation_map += ("left_node_name" -> scan_site_names(i))
            relation_map += ("lnode_come_date" -> come_dates(i))
            relation_map += ("lnode_send_date" -> send_dates(i))
            relation_map += ("lnode_scan_date" -> scan_dates(i))

            relation_map += ("right_node_name" -> scan_site_names(i + 1))
            relation_map += ("rnode_come_date" -> come_dates(i + 1))
            relation_map += ("rnode_send_date" -> send_dates(i + 1))
            relation_map += ("rnode_scan_date" -> scan_dates(i + 1))
            node_relations += relation_map
          }
        }


        nodes.foreach((node: Map[String, String]) => {
         // crate_node(neo4jConfig, node("node_id"), node("node_name"), node("ategory_code"))
        })

        node_relations.foreach((node_relation: Map[String, String]) => {
//          create_relation(neo4jConfig, node_relation("ds"), node_relation("bill_code"),
//            node_relation("left_node_name"), node_relation("right_node_name"),
//            node_relation("lnode_come_date"), node_relation("lnode_send_date"), node_relation("lnode_scan_date"),
//            node_relation("rnode_come_date"), node_relation("rnode_send_date"), node_relation("rnode_scan_date")
//          )
        })

      })
    })
  }


//  def crate_node(conf:org.neo4j.spark.Neo4jConfig, node_id:AnyRef, node_name:AnyRef, category_code:AnyRef): Unit ={
//    val cql = "merge (n:Node {id:{node_id}, name:{node_name}, category_code:{category_code}}) on create set n.created = timestamp()"
//    val params = scala.Seq("node_id" -> node_id, "node_name" -> node_name, "category_code" -> category_code)
//    Neo4jGraph.execute(conf,cql,params)
//  }
//
//  def create_relation(conf:org.neo4j.spark.Neo4jConfig, ds:AnyRef, bill_code:AnyRef, left_node_name:AnyRef, right_node_name:AnyRef,
//                      lnode_come_date:AnyRef, lnode_send_date:AnyRef, lnode_scan_date:AnyRef,
//                      rnode_come_date:AnyRef, rnode_send_date:AnyRef, rnode_scan_date:AnyRef): Unit ={
//    val cql = "match (a:Node {name:{left_node_name}}),(b:Node {name:{right_node_name}}) " +
//      "merge (a) -[r:b_"+bill_code+" {bill_code: {bill_code},ds: {ds}," +
//      "lnode_come_date: {lnode_come_date},lnode_send_date: {lnode_send_date},lnode_scan_date: {lnode_scan_date}," +
//      "rnode_come_date: {rnode_come_date},rnode_send_date: {rnode_send_date},rnode_scan_date: {rnode_scan_date}}]->(b)"
//    val params = scala.Seq("ds" ->ds, "bill_code" -> bill_code,"left_node_name" -> left_node_name,"right_node_name" -> right_node_name,
//      "lnode_come_date" -> lnode_come_date,"lnode_send_date" -> lnode_send_date,"lnode_scan_date" -> lnode_scan_date,
//      "rnode_come_date" -> rnode_come_date,"rnode_send_date" -> rnode_send_date,"rnode_scan_date" -> rnode_scan_date)
//    Neo4jGraph.execute(conf,cql,params)
//  }

}
