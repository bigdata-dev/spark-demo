package sparkhbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tonye0115 on 2017/3/21.
  */
object HBaseWriter {

  """
      |create 'userinfo', {NAME => 'info', VERSIONS => 10}
    |
    |spark-submit \
    |--class sparkhbase.HBaseWriter \
    |--master yarn-client \
    |/var/lib/hadoop-hdfs/spark-work/spark-demo.jar
  """.stripMargin
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println( s"""
                             |Usage: HBaseWriter <hbase.zookeeper.quorum> <hbase.zookeeper.property.clientPort>
                             |  <hbase.zookeeper.quorum> is hbase zookeeper node
                             |  <hbase.zookeeper.property.clientPort> is hbase zookeeper port
                             |  <input> is hdfs input path
                             |
                             |spark-submit \
                             |--class sparkhbase.HBaseWriter \
                             |--master yarn \
                             |--deploy-mode client \
                             |/var/lib/hadoop-hdfs/spark-work/spark-demo.jar \
                             |cdh10.ryxc,cdh11.ryxc,cdh20.ryxc \
                             |2181 \
                             |hdfs://nameservice1/library/userinfo/20170321/*.txt
                             |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.INFO)

    val Array(quorum, clientPort, input) = args

    val conf = new SparkConf().setAppName("HBaseWriter")
    val sc = new SparkContext(conf)

//    sc.schedulerBackend.applicationAttemptId()

    //定义 HBase 的配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", quorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)

    val tableName = "userinfo"
    val colfamily = "info"


    val rdd = sc.textFile(input)
    val rawRDD = rdd.map(s => {
      val split = s.split("\t")
      val rowkey = split(0)+split(1)+split(2)
      (hashMd5(rowkey),split(0).toInt, split(1), split(2).toInt)
    })

    //指定输出格式和输出表名
    val jobConf = new JobConf(hbaseConf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    //存储到hbase
    rawRDD.map(convert(colfamily,_)).saveAsHadoopDataset(jobConf)

  }

  def convert(colfamily: String, triple: (String, Int, String, Int)) = {
    val p = new Put(Bytes.toBytes(triple._1))
    p.addColumn(Bytes.toBytes(colfamily),Bytes.toBytes("id"),Bytes.toBytes(triple._2))
    p.addColumn(Bytes.toBytes(colfamily),Bytes.toBytes("name"),Bytes.toBytes(triple._3))
    p.addColumn(Bytes.toBytes(colfamily),Bytes.toBytes("age"),Bytes.toBytes(triple._4))
    (new ImmutableBytesWritable, p)
  }

  def hashMd5(s:String)={
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b,0,b.length)
    new java.math.BigInteger(1,m.digest()).toString(16)
  }

}


