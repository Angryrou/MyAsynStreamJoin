package cluster

import scala.collection.concurrent.TrieMap

/**:
  * Created by kawhi on 09/08/2017.
  */
object APKMate {
  // 这个 object 会在 executor 中常驻. 要注意
  //  当某个 batch 中的某个 partition 没有了信息之后, 会不会因为上一个batch 的信息对该 batch 产生影响
  //  答案是不会的. 因为如果 partition 没有信息,在做 getPartition 函数中, 并不会有其对应的 pid 出现.
  private val headTable = TrieMap[Int, Map[BigInt, Set[String]]]()

  def updateHeadTable(partitionId: Int, head: Map[BigInt, Set[String]]): Unit = {
    headTable(partitionId) = head
  }

//  def updateHeadTableString(partitionId: Int, headStr: String): Unit = {
//    headTable(partitionId) = headStr.split(",").toSet
//  }

  def getAllHead(pid: Int): Map[BigInt, Set[String]] = {
    headTable(pid)
  }

  def getEtwHead(pid: Int, etw: BigInt): Set[String] = {
    headTable(pid).getOrElse(pid, Set[String]())
  }
}
