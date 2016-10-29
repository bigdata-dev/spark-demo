package sparkstreaming.kafka.producer


import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
  * Created by admin on 2016/10/29.
  */
class UserBehaviorMsgProducer(brokers:String,topic:String) extends Runnable{
  private val brokerList: String = brokers
  private val targetTopic: String = topic
    
  private val MAX_MSG_NUM: Int = 3
  private val PAGE_NUM: Int = 100
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
          msg.append("page" + (random.nextInt(PAGE_NUM)))
          msg.append("|")
          msg.append(random.nextInt(MAX_CLICK_TIME) + 1)
          msg.append("|")
          msg.append(random.nextInt(MAX_CLICK_TIME) + random.nextFloat())
          msg.append("|")
          msg.append(LIKE_OR_NOT(random.nextInt(3)))
          sendMessage(msg.toString)
        }
        println("%d user behavior messages produced.".format(msgNum+1))
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
