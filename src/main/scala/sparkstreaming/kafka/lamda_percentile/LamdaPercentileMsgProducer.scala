package sparkstreaming.kafka.lamda_percentile

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
  * Created by admin on 2016/10/29.
  */
class LamdaPercentileMsgProducer(brokers:String, topic:String) extends Runnable{
  private val brokerList: String = brokers

  private val MAX_MSG_NUM: Int = 30

  //浏览器类型
  private val IETYPE_NUM: Int = 3
  //页面url
  private val PAGEURL_NUM: Int = 100
  //加载时间(秒)
  private val LOADTIME_NUM: Int = 50


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
          msg.append("ietype" + (random.nextInt(IETYPE_NUM)))
          msg.append("|")
          msg.append("page" + (random.nextInt(PAGEURL_NUM)))
          msg.append("|")
          msg.append((random.nextInt(LOADTIME_NUM)))
          println(msg.toString)
          sendMessage(msg.toString)
        }
        println("%d lamda percentile messages produced.".format(msgNum+1))
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
  * kafka-topics --create --zookeeper hadoop10.zto:2181/kafka --replication-factor 3 --partitions 3 --topic lamda-percentile-topic
  * kafka-topics --create --zookeeper ryxc163:2181 --replication-factor 3 --partitions 3 --topic lamda-percentile-topic
  * 生产message:
  * kafka-console-producer --broker-list 10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092 --topic lamda-percentile-topic
  * kafka-console-producer --broker-list ryxc163:9092,ryxc164:9092,ryxc165:9092 --topic lamda-percentile-topic
  * 消费message:
  * kafka-console-consumer --zookeeper hadoop10.zto:2181/kafka --topic lamda-percentile-topic
  * kafka-console-consumer --zookeeper ryxc163:2181 --topic lamda-percentile-topic
  */
object LamdaPercentileMsgProducer{
  def main(args: Array[String]) {
    if(args.length < 2){
      println("Usage LamdaPercentileMsgProducer 10.9.12.21:9092,10.9.12.22:9092,10.9.12.23:9092 lamda-percentile-topic")
      System.exit(1)
    }
    new Thread(new LamdaPercentileMsgProducer(args(0),args(1))).start()
  }
}
