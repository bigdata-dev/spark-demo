package utils


import org.neo4j.driver.v1.{AuthTokens, Config, Driver, GraphDatabase}

/**
  * Created by tonye0115 on 2017/11/15.
  */
class Neo4jDBManager(url:String, user:String,passwd:String) extends Serializable{
  //val conf = Config.build().withMaxIdleSessions(500).toConfig
  val driver = GraphDatabase.driver(url,AuthTokens.basic(user,passwd))
  def getDriver: Driver={
    try {
      return driver
    } catch {
      case ex:Exception => ex.printStackTrace()
        print(ex)
        null
    }
  }

}

object Neo4jDBManager{
  var neo4jDBManager:Neo4jDBManager=_
  def getNeo4jDBManager(url:String, user:String,passwd:String):Neo4jDBManager={
    synchronized{
      if(neo4jDBManager==null){
        neo4jDBManager = new Neo4jDBManager(url, user, passwd)
      }
    }
    neo4jDBManager
  }
}
