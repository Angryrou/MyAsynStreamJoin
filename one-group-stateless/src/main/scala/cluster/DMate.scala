package cluster

import scala.collection.concurrent.TrieMap

/**
  * Created by kawhi on 22/09/2017.
  */
object DMate {

  // 这个 object 会在 executor 中常驻. 要注意
  //  当某个 batch 中的某个 partition 没有了信息之后, 会不会因为上一个batch 的信息对该 batch 产生影响
  //  答案是不会的. 因为如果 partition 没有信息,在做 getPartition 函数中, 并不会有其对应的 pid 出现.
  private val headTable = TrieMap[Int, Set[String]]()

  def updateHeadTable(partitionId: Int, head: Set[String]): Unit = {
    headTable(partitionId) = head
  }
  def getHead(pid: Int): Set[String] = {
    headTable(pid)
  }

  var lambda = 0.0 // maximum load / dispersion
  var m = 15 // mapper size
  var M = 150000 * 3
  private val delta = 0.0d to 10.0d by 0.1d toArray // delta
  private val h_delta = delta.map(x => (1+x) * Math.log(1+x) - x) // h_delta

  var p1 = -0.1 // biggest possibility of z
  var K = 0 // key size

}