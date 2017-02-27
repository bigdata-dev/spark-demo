package sparksql

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.util.Random

/**
  * Created by tonye0115 on 2017/2/27.
  * 论坛数据自动生成代码
  * 数据格式：
  * date: 日期格式为yyyy-MM-dd
  * timestamp: 时间戳
  * userID: 用户ID
  * pageID: 页面ID
  * channelID:板块ID
  * action:点击和注册
  *
  */
object SparkSQLGenManually {
  //具体的论坛频道
  val channelNames = Array(
    "spark","scala","kafka","flink","hadoop",
    "strom","hive","impala", "hbase","ml"
  )

  val actionNames = Array(
    "View","Register"
  )

  //昨天的时间的生成
  val yesterdayFormated =  getYesterday()

  val userLogBuffer = new StringBuilder
  val random: Random = new Random()

  def userLogs(numberItems: Int, path: String): Unit = {

    for(i <- 0 to numberItems){
      val timestamps = new Date().getTime
      //随机生成的用户ID
      val userID = random.nextInt(numberItems)
      //随机生成的页面ID
      val pageID = random.nextInt(numberItems)
      //随机生成channel
      val channel = channelNames(random.nextInt(channelNames.length))
      //随机生成action
      val action = actionNames(random.nextInt(actionNames.length))
      userLogBuffer.append(yesterdayFormated)
        .append("\t")
        .append(timestamps)
        .append("\t")
        .append(userID)
        .append("\t")
        .append(pageID)
        .append("\t")
        .append(channel)
        .append("\t")
        .append(action)
        .append("\n")


    }
  }

  def main(args: Array[String]) {

    var numberItems:Int = 5000
    var path = ""
    if(args.length>0) {
      numberItems = Integer.valueOf(args(0))
      path = args(1)
    }

    println("User log number :"+ numberItems)

    //生成数据userLogs
    userLogs(numberItems, path)
    //print(userLogBuffer)
    val pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path+"\\userLog.log")))
    try {
      pw.write(userLogBuffer.toString())
    } finally{
      pw.close()
    }

  }


  def getYesterday(): String = {
    val sdf =  new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    return sdf.format(cal.getTime)
  }
}
