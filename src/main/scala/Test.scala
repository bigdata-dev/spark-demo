import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * Created by tangfei on 2016/11/18.
  */
object Test {
  def main(args: Array[String]) {
    val a:Option[Int] = Some(5)
    val b:Option[Int] = None

   // println("a.getOrElse(0): " + a.getOrElse(a,10) )
   // println("b.getOrElse(10): " + b.getOrElse(b,100) )

    val resultMap = new HashMap[Seq[String],HashMap[Int,Int]]
    val tmp: (Seq[String], Int) = (Seq("A","B"),1)
    println(tmp._1)
    val colors = HashMap(1 -> 1)
    resultMap.put(tmp._1,colors)
    val valueMap: mutable.HashMap[Int, Int] = resultMap.getOrElse(tmp._1,new HashMap[Int,Int])
    valueMap.toList.foreach(println)
  }
}
