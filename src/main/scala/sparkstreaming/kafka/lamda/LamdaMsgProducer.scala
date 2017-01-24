package sparkstreaming.kafka.lamda

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
  * Created by admin on 2016/10/29.
  */
class LamdaMsgProducer(brokers:String, topic:String) extends Runnable{
  private val brokerList: String = brokers

  private val MAX_MSG_NUM: Int = 30

  //站点ID
  private val SITE_NUM: Int = 3
  //访客ID
  private val VISITOR_NUM: Int = 10000
  //页面url
  private val PAGEURL_NUM: Int = 100
  private val MAX_CLICK_TIME: Int = 5
  private val LIKE_OR_NOT: Array[Int] = Array[Int](1,0,-1)

  private val props: Properties = new Properties()
  props.put("metadata.broker.list",this.brokerList)
  props.put("serializer.class","kafka.serializer.StringEncoder")
  props.put("produer.type","async")
  private val config: ProducerConfig = new ProducerConfig(props)
  private val producer: Producer[String, String] = new Producer[String,String](config)

  def sendMessage(message: String) = {
    try {
      val data: KeyedMessage[String, String] = new KeyedMessage[String, String](this.topic, message)
      producer.send(data)
    } catch {
      case e:Exception => println(e)
    }
  }


  override def run(): Unit = {
    val random: Random = new Random()
    while (true) {
      val msgNum: Int = random.nextInt(MAX_MSG_NUM) + 1
      try {
        for (i <- 0 to msgNum) {
          val msg: StringBuffer = new StringBuffer()
          msg.append("site" + (random.nextInt(SITE_NUM)))
          msg.append("|")
          msg.append("visitor" + (random.nextInt(VISITOR_NUM)))
          msg.append("|")
          msg.append("page" + (random.nextInt(PAGEURL_NUM))+".html")
          println(msg.toString)
          sendMessage(msg.toString)
        }
        println("%d lamda messages produced.".format(msgNum+1))
      } catch {
        case e:Exception => println(e)
      }

      try {
        Thread.sleep(5000)
      } catch {
        case e:Exception => println(e)
      }
    }
  }



}

/**
  * 创建topic:
  * kafka-topics --create --zookeeper ryxc163:2181 --replication-factor 3 --partitions 3 --topic lamda-topic
  * 生产message:
  * kafka-console-producer --broker-list ryxc163:9092,ryxc164:9092,ryxc165:9092 --topic lamda-topic
  * 消费message:
  * kafka-console-consumer --zookeeper ryxc163:2181 --topic lamda-topic
  */
object LamdaMsgProducer{
  def main(args: Array[String]) {
    if(args.length < 2){
      println("Usage LamdaMsgProducer ryxc163:9092,ryxc164:9092,ryxc165:9092 lamda-topic")
      System.exit(1)
    }
    new Thread(new LamdaMsgProducer(args(0),args(1))).start()
  }
}
