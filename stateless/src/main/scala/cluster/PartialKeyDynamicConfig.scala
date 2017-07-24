package cluster

import scala.collection.concurrent.TrieMap

/**
  * Created by kawhi on 03/07/2017.
  */
object PartialKeyDynamicConfig {
  private val dTables = TrieMap[Int, Int]()
  private val seedsArray = Array(1,2,4,5,6,8,11,12,13,15,16,19,20,21,22,24,26,28)

  def updateDTables(partitionId: Int, d: Int): Unit = {
    dTables(partitionId) = d
  }

  def getGlobalSeeds(d: Int) : Array[Int] = {
    return seedsArray.slice(0, if (d > 2) d else 2)

  }

  def getSeeds() : Array[Int] = {
    println(dTables.mkString(","))
    val d = if (dTables.size == 0) 2 else {
      val tmp = Math.max(dTables.values.max, 2)
      Math.min(tmp, seedsArray.length)
    }
    println(s"partition-d : $d")
    dTables.clear()
    return seedsArray.slice(0, d)
  }

}
