package mypartitioner

import com.google.common.hash.Hashing
import org.apache.spark.{Partitioner, TaskContext}
import cluster.AdvancedConfig

/**
  * Created by kawhi on 24/07/2017.
  */
class AdvancedPartialKeyPartitioner(partitions : Int, seeds : Array[Int]) extends Partitioner{

  private val tailSeeds = Array[Int](1,2)

  private val tailHashes = tailSeeds.map(Hashing.murmur3_128(_))
  private val headHashes = seeds.map(Hashing.murmur3_128(_))
  private val mapperStats = new Array[Int](partitions)
  private var head = Set[String]()
  private var pid = -1

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    if (pid != TaskContext.getPartitionId()){
      pid = TaskContext.getPartitionId()
      head = AdvancedConfig.getHead(pid)
    }
    val skey = key.toString

    val choices = if (head.contains(skey)) {
      headHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
    } else {
      tailHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
    }

    var ret = choices(0)
    choices.foreach(c => {
      if (mapperStats(ret) > mapperStats(c))
        ret = c
    })
    mapperStats(ret) += 1
    ret
  }

  override def equals(other: Any): Boolean = other match {
    case h: PartialKeyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
