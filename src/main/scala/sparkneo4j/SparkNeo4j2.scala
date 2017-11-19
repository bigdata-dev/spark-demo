package sparkneo4j



import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1.Values
import utils.Neo4jDBManager

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tonye0115 on 2017/11/13.
  */
object SparkNeo4j2 {
  """
    |spark-submit \
    |--master yarn \
    |--deploy-mode client \
    |--num-executors 50 \
    |--executor-memory 5G \
    |--conf spark.neo4j.bolt.url=bolt://0.0.0.0:7687 \
    |--conf spark.neo4j.bolt.user=neo4j \
    |--conf spark.neo4j.bolt.password=***  \
    |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-java-driver-1.1.0.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/graphframes-0.1.0-spark1.6.jar \
    |--class sparkneo4j.SparkNeo4j2 \
    |/home/tonye/spark-work/spark-demo.jar \
    |20170915 node
    |
    |spark-submit \
    |--master local \
    |--executor-memory 2G \
    |--conf spark.neo4j.bolt.url=bolt://0.0.0.0:7687 \
    |--conf spark.neo4j.bolt.user=neo4j \
    |--conf spark.neo4j.bolt.password=***  \
    |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-java-driver-1.1.0.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/graphframes-0.1.0-spark1.6.jar \
    |--class sparkneo4j.SparkNeo4j2 \
    |/home/tonye/spark-work/spark-demo.jar \
    |20170915 node
    |
  """.stripMargin
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        """
        |Usage: <ds> <job_name("node" or "relation")>
        |""")
    }

    val Array(ds, job_name) = args
    val conf = new SparkConf().setAppName("SparkNeo4j")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    val url = sc.getConf.get("spark.neo4j.bolt.url")
    val user = sc.getConf.get("spark.neo4j.bolt.user")
    val passwd = sc.getConf.get("spark.neo4j.bolt.password")



    val resultDF = hiveContext.sql(
      """
          |select bill_code,scan_site_id,fullname,categorycode,scan_date,come_date,send_date from tmp.tmp_vrp
        """.stripMargin
    )

    resultDF.collect().foreach(println)

    if (job_name=="node"){
      resultDF.foreachPartition(it=>{
        val driver = Neo4jDBManager.getNeo4jDBManager(url, user, passwd).getDriver
        it.foreach(row =>{
          var nodes = ArrayBuffer[Map[String,String]]()
          val bill_code = row.getAs[String]("bill_code")
          val scan_site_ids = row.getAs[String]("scan_site_id").split('|')
          val scan_site_names = row.getAs[String]("fullname").split('|')
          val category_codes = row.getAs[String]("categorycode").split('|')
          val scan_dates = row.getAs[String]("scan_date").split('|')
          val send_dates = row.getAs[String]("send_date").split('|')
          val come_dates = row.getAs[String]("come_date").split('|')
          for (i <- 0 to scan_site_ids.length - 1) {
            var node_map = Map[String, String]()
            node_map += ("node_id" -> scan_site_ids(i))
            node_map += ("node_name" -> scan_site_names(i))
            node_map += ("category_code" -> category_codes(i))
            nodes += node_map
          }

          nodes.foreach((node: Map[String, String]) => {
            create_node(driver,node("node_id"),node("node_name"),node("category_code"))
          })

        })

      })
    }else{
      resultDF.foreachPartition(it=>{
        val driver = Neo4jDBManager.getNeo4jDBManager(url, user, passwd).getDriver
        it.foreach(row =>{
          val bill_code = row.getAs[String]("bill_code")
          val scan_site_ids = row.getAs[String]("scan_site_id").split('|')
          val scan_site_names = row.getAs[String]("fullname").split('|')
          val node_relations = ArrayBuffer[Map[String,String]]()
          for (i <- 0 to scan_site_ids.length - 2) {
            var relation_map = Map[String, String]()
            relation_map += ("ds" -> ds)
            relation_map += ("left_node_name" -> scan_site_names(i))
            relation_map += ("right_node_name" -> scan_site_names(i + 1))
            node_relations += relation_map
          }
          node_relations.foreach((node_relation: Map[String, String]) => {
            create_relation(driver, node_relation("ds"),
              node_relation("left_node_name"), node_relation("right_node_name")
            )
          })
        })
        driver.close
      })
    }

  }



  def create_node(driver:org.neo4j.driver.v1.Driver,node_id:AnyRef, node_name:AnyRef, category_code:AnyRef): Unit ={
    val session = driver.session()
    val tx = session.beginTransaction()
    try{
        val cql = "merge (n:Node {id:{node_id}, name:{node_name}, category_code:{category_code}}) on create set n.created = timestamp()"
        val params = Values.parameters("node_id",node_id,"node_name",node_name,"category_code",category_code)
        tx.run(cql, params)
        tx.success()
      }catch {
        case ex: Exception=>
          print(ex)
          tx.failure()
      }finally {
        tx.close()
        session.close()
      }
  }


  def create_relation(driver:org.neo4j.driver.v1.Driver, ds:AnyRef, left_node_name:AnyRef, right_node_name:AnyRef): Unit ={
    val session = driver.session()
    val tx = session.beginTransaction()
    try {
      val cql = "match (a:Node {name:{left_node_name}}),(b:Node {name:{right_node_name}}) " +
        "merge (a) -[r:r_" + ds + "_send]->(b)" +
        "on create set r.bill_code_count=0 " +
        "set r.bill_code_count=r.bill_code_count+1";
      val params = Values.parameters("ds", ds, "left_node_name", left_node_name, "right_node_name", right_node_name)
      tx.run(cql, params)
      tx.success()
    }catch {
      case ex: Exception=>
        print(ex)
        tx.failure()
    }finally {
      tx.close()
      session.close()
    }

  }

}
