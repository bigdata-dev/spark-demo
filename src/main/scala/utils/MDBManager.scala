package utils

import java.io.{File, FileInputStream, InputStream}
import java.sql.Connection
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.SparkFiles

/**
  * Created by tangfei on 2016/11/3.
  */
class MDBManager(isLocal:Boolean) extends Serializable{
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true);
  private val prop = new Properties()
  private var in:InputStream = _
  isLocal match{
    //spark-clilent模式下 如果为true 将c3p0.properties 上传到/opt/cloudera/parcels/CDH/lib/spark/conf目录下
    case true  => in = getClass().getResourceAsStream("/c3p0.properties");
    case false => in = new FileInputStream(new File(SparkFiles.get("c3p0.properties")))
  }
  try {
    prop.load(in);
    cpds.setJdbcUrl(prop.getProperty("jdbcUrl").toString());
    cpds.setDriverClass(prop.getProperty("driverClass").toString());
    cpds.setUser(prop.getProperty("user").toString());
    cpds.setPassword(prop.getProperty("password").toString());
    cpds.setMaxPoolSize(Integer.valueOf(prop.getProperty("maxPoolSize").toString()));
    cpds.setMinPoolSize(Integer.valueOf(prop.getProperty("minPoolSize").toString()));
    cpds.setAcquireIncrement(Integer.valueOf(prop.getProperty("acquireIncrement").toString()));
    cpds.setInitialPoolSize(Integer.valueOf(prop.getProperty("initialPoolSize").toString()));
    cpds.setMaxIdleTime(Integer.valueOf(prop.getProperty("maxIdleTime").toString()));
  } catch {
    case ex: Exception => ex.printStackTrace()
  }
  def getConnection:Connection={
    try {
      return cpds.getConnection();
    } catch {
      case ex:Exception => ex.printStackTrace()
        null
    }
  }
}
object MDBManager{
  var mdbManager:MDBManager=_
  def getMDBManager(isLocal:Boolean):MDBManager={
    synchronized{
      if(mdbManager==null){
        mdbManager = new MDBManager(isLocal)
      }
    }
    mdbManager
  }
}
