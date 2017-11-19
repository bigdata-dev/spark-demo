package sparkneo4j

import java.sql.DriverManager

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkContext
//import org.neo4j.spark.{Neo4jGraph, Neo4jRowRDD}



/**
  * Created by tonye0115 on 2017/11/9.
  */
object Test {
//  import org.neo4j.spark._
//  def crate_node(conf:org.neo4j.spark.Neo4jConfig, node_name:AnyRef, category_code:AnyRef): Unit ={
//    val cql = "merge (n:Node {name:{start_node_name},category_code:{category_code}}) on create set n.created = timestamp()"
//    val params = scala.Seq("start_node_name" -> node_name, "category_code" -> category_code)
//    Neo4jGraph.execute(conf,cql,params)
//  }
//
//  def create_relation(conf:org.neo4j.spark.Neo4jConfig, ds:AnyRef, bill_code:AnyRef, start_node_name:AnyRef, end_node_name:AnyRef, start_time:AnyRef, end_time:AnyRef, cost_time:AnyRef): Unit ={
//    val cql = "match (a:Node {name:{start_node_name}}),(b:Node {name:{end_node_name}}) " +
//      "merge (a) -[r:b_"+bill_code+" {start_time: {start_time},end_time: {end_time},cost_time: {cost_time},bill_code: {bill_code},ds: {ds}}]->(b);"
//    val params = scala.Seq("ds" ->ds, "bill_code" -> bill_code,"start_node_name" -> start_node_name,"end_node_name" -> end_node_name,"start_time" -> start_time, "end_time" -> end_time, "cost_time" -> cost_time)
//    Neo4jGraph.execute(conf,cql,params)
//  }
//
//
//
//  def main(args: Array[String]) {
//    val sc = new SparkContext()
//    val neo4jConfig = new Neo4jConfig(sc.getConf.get("spark.neo4j.bolt.url"),
//      sc.getConf.get("spark.neo4j.bolt.user"),
//      Option(sc.getConf.get("spark.neo4j.bolt.password")))
//    //val jsonString = "{\n    \"bill_code\": \"537599768183\",\n    \"ds\": \"20171110\",\n    \"data\": [\n        {\n            \"start_node\": {\n                \"name\": \"焦作温县\",\n                \"category_code\": \"二级网点\"\n            },\n            \"end_node\": {\n                \"name\": \"郑州\",\n                \"category_code\": \"一级网点\"\n            },\n            \"start_time\": \"2017-11-06 19:08:32\",\n            \"end_time\": \"2017-11-06 19:08:32\",\n            \"cost_time\": \"222\"\n        },\n        {\n            \"start_node\": {\n                \"name\": \"郑州\",\n                \"category_code\": \"一级网点\"\n            },\n            \"end_node\": {\n                \"name\": \"郑州中转\",\n                \"category_code\": \"一级网点\"\n            },\n            \"start_time\": \"2017-11-06 19:08:32\",\n            \"end_time\": \"2017-11-06 19:08:32\",\n            \"cost_time\": \"222\"\n        },\n        {\n            \"start_node\": {\n                \"name\": \"郑州中转\",\n                \"category_code\": \"一级网点\"\n            },\n            \"end_node\": {\n                \"name\": \"二七网点\",\n                \"category_code\": \"一级网点\"\n            },\n            \"start_time\": \"2017-11-06 19:08:32\",\n            \"end_time\": \"2017-11-06 19:08:32\",\n            \"cost_time\": \"222\"\n        }\n    ]\n}"
//
//    val jsonString = "{\n    \"bill_code\": \"537599768185\",\n    \"ds\": \"20171110\",\n    \"data\": [\n        {\n            \"start_node\": {\n                \"name\": \"焦作温县\",\n                \"category_code\": \"二级网点\"\n            },\n            \"end_node\": {\n                \"name\": \"郑州\",\n                \"category_code\": \"一级网点\"\n            },\n            \"start_time\": \"2017-11-06 19:08:32\",\n            \"end_time\": \"2017-11-06 19:08:32\",\n            \"cost_time\": \"222\"\n        },\n        {\n            \"start_node\": {\n                \"name\": \"郑州\",\n                \"category_code\": \"一级网点\"\n            },\n            \"end_node\": {\n                \"name\": \"中心2\",\n                \"category_code\": \"一级网点\"\n            },\n            \"start_time\": \"2017-11-06 19:08:32\",\n            \"end_time\": \"2017-11-06 19:08:32\",\n            \"cost_time\": \"222\"\n        },\n        {\n            \"start_node\": {\n                \"name\": \"中心2\",\n                \"category_code\": \"一级网点\"\n            },\n            \"end_node\": {\n                \"name\": \"二七网点\",\n                \"category_code\": \"一级网点\"\n            },\n            \"start_time\": \"2017-11-06 19:08:32\",\n            \"end_time\": \"2017-11-06 19:08:32\",\n            \"cost_time\": \"222\"\n        }\n    ]\n}"
//
//
//    val jsonObject = com.alibaba.fastjson.JSON.parseObject(jsonString)
//    val bill_code = jsonObject.get("bill_code")
//    val ds = jsonObject.get("ds")
//    val array = jsonObject.get("data").asInstanceOf[com.alibaba.fastjson.JSONArray]
//    import org.neo4j.spark._
//    for (i <- 0 to array.size()-1) {
//      val path_data = array.get(i).asInstanceOf[com.alibaba.fastjson.JSONObject]
//      val start_node_name = path_data.get("start_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("name")
//      val start_node_category_code = path_data.get("start_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("category_code")
//      val end_node_name = path_data.get("end_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("name")
//      val end_node_category_code = path_data.get("end_node").asInstanceOf[com.alibaba.fastjson.JSONObject].get("category_code")
//      val start_time = path_data.get("start_time")
//      val end_time = path_data.get("end_time")
//      val cost_time = path_data.get("cost_time")
//
//      crate_node(neo4jConfig,start_node_name,start_node_category_code)
//      crate_node(neo4jConfig,end_node_name,end_node_category_code)
//      create_relation(neo4jConfig, ds, bill_code, start_node_name, end_node_name,start_time,end_time, cost_time)
//    }
//
//
//
//
//
//
//
//
//
//
//
//
//
//  }


  def main(args: Array[String]) {
    Class.forName("org.neo4j.jdbc.Driver")
    val conn = java.sql.DriverManager.getConnection("jdbc:neo4j://0.0.0.0:7474/",
      "neo4j","neo4j")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("MATCH (n) RETURN n");
    while(rs.next()){
      System.out.println(rs.getString("n"));
    }


  }

}
