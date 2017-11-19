package sparkneo4j

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import utils.MDBManager

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by tonye0115 on 2017/11/13.
  */
object SparkNeo4jJdbc {
  """
    |spark-submit \
    |--master yarn \
    |--deploy-mode client \
    |--num-executors 2 \
    |--executor-memory 5G \
    |--files /home/tonye/spark-work/neo4j_c3p0.properties \
    |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-jdbc-2.0.1-SNAPSHOT-jar-with-dependencies.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/c3p0-0.9.5.2.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/mchange-commons-java-0.2.11.jar \
    |--packages org.codehaus.jackson:jackson-core-asl:1.9.2 \
    |--class sparkneo4j.SparkNeo4jJdbc \
    |/home/tonye/spark-work/spark-demo.jar \
    |20170915 node
    |
    |
    |spark-submit \
    |--master local \
    |--executor-memory 2G \
    |--files /home/tonye/spark-work/neo4j_c3p0.properties \
    |--jars /opt/cloudera/parcels/CDH/lib/spark/lib/neo4j-jdbc-2.3.2-jar-with-dependencies,/opt/cloudera/parcels/CDH/lib/spark/lib/c3p0-0.9.5.2.jar,/opt/cloudera/parcels/CDH/lib/spark/lib/mchange-commons-java-0.2.11.jar \
    |--class sparkneo4j.SparkNeo4jJdbc \
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

//    val resultDF = hiveContext.sql(
//      """
//          |select bill_code,scan_site_id,fullname,categorycode,scan_date,come_date,send_date from tmp.tmp_vrp limit 1
//        """.stripMargin
//    )


      val resultDF = hiveContext.sql(
        """
            |select bill_code,scan_site_id,fullname,categorycode from tmp.tmp_vrp
          """.stripMargin
      )

    //resultDF.collect().foreach(println)

    if (job_name=="node"){
      resultDF.foreachPartition(it=>{
        //从连接池中获取一个连接
        val conn = MDBManager.getMDBManager(false).getConnection
        conn.setAutoCommit(true)

        val cql =
          """
            |merge (n:Node {id:{1}, name:{2}, category_code:{3}}) on create set n.created = timestamp()
          """.stripMargin
        val preparedStatement = conn.prepareStatement(cql)
        it.foreach(row =>{
          val bill_code = row.getAs[String]("bill_code")
          val scan_site_ids = row.getAs[String]("scan_site_id").split('|')
          val scan_site_names = row.getAs[String]("fullname").split('|')
          val category_codes = row.getAs[String]("categorycode").split('|')
//          val scan_dates = row.getAs[String]("scan_date").split('|')
//          val send_dates = row.getAs[String]("send_date").split('|')
//          val come_dates = row.getAs[String]("come_date").split('|')

          for (i <- 0 to scan_site_ids.length - 1) {
//            var node_map = Map[String, String]()
//            node_map += ("node_id" -> scan_site_ids(i))
//            node_map += ("node_name" -> scan_site_names(i))
//            node_map += ("ategory_code" -> category_codes(i))
//
//            nodes += node_map

            preparedStatement.setObject(1, scan_site_ids(i))
            preparedStatement.setObject(2, scan_site_names(i))
            preparedStatement.setObject(3, category_codes(i))
            preparedStatement.execute()
          }

//          if(nodes.length/1000==1){
//            preparedStatement.executeBatch()
//            nodes.clear()
//          }

        })

//        if(nodes.length!=0){
//          preparedStatement.executeBatch()
//          nodes.clear()
//        }
//       conn.commit()
        conn.close()
      })
    }else{
      resultDF.foreachPartition(it=>{
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
          })
        })
      })
    }

  }

}
