package sparkstreaming.socket

import java.io.PrintWriter
import java.net.ServerSocket
import scala.collection.mutable.ListBuffer

/**
 * Created by  ryxc 15/6/9.
 */
object Sender {

  def generateContent(index: Int) :String = {
    val charList = ListBuffer[Char]()
    for(i <- 65 to 90){// A B C D ...
      charList += i.toChar
    }
    val charArray = charList.toArray
    charArray(index).toString
  }

  //随机数
  def index = {
    import java.util.Random
    val rdm = new Random()
    rdm.nextInt(7)
  }

  def main(args: Array[String]) {
    if(args.length != 2){
      //参数一是发送的端口  参数二是发送的时间间隔  单位是毫秒
      System.err.println("usage: <port> <millisecond>")
      System.exit(1)
    }
    val listener = new ServerSocket(args(0).toInt)
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(),true)
          while(true){
            Thread.sleep(args(1).toLong)
            val content = generateContent(index)
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
