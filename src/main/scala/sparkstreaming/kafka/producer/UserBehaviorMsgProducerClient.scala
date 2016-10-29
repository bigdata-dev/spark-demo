package sparkstreaming.kafka.producer

/**
  * Created by admin on 2016/10/30.
  */
object UserBehaviorMsgProducerClient {
  def main(args: Array[String]) {
    if(args.length < 2){
      println("Usage userBehaviorMsgProducerClient ryxc164:9092 user-behavior-topic")
      System.exit(1)
    }
    new Thread(new UserBehaviorMsgProducer(args(0),args(1))).start()
  }
}
