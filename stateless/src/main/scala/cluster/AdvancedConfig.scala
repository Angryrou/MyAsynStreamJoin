package cluster

import scala.collection.concurrent.TrieMap

/**
  * Created by kawhi on 24/07/2017.
  */
object AdvancedConfig {
  private val headTable = TrieMap[Int, Set[String]]()

  def updateHeadTable(partitionId: Int, head: Set[String]): Unit = {
    headTable(partitionId) = head
  }

  def getHead(pid: Int): Set[String] = {
    headTable(pid)
  }
}
