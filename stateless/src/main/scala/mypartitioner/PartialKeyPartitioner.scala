package mypartitioner

import com.google.common.hash.Hashing
import org.apache.spark.{Partitioner, TaskContext}

import scala.collection.mutable
/**
  * Created by kawhi on 23/05/2017.
  */
class PartialKeyPartitioner(partitions : Int, seeds : Array[Int]) extends Partitioner {
  // 每个
  private val hashes = seeds.map(Hashing.murmur3_128(_))
  private val mapperStatsSet = mutable.Map[Int, Array[Int]]() // 每个 executor 中的每个 partition 都有一个维护自己的 Array

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    val pid = TaskContext.getPartitionId()
    val mapperStats = mapperStatsSet.getOrElse(pid, new Array[Int](partitions))
    val choices = hashes.map(h => (Math.abs(h.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt)
    var ret = choices(0)
    choices.foreach(c => {
      if (mapperStats(ret) > mapperStats(c))
        ret = c
    })
    mapperStats(ret) += 1
    mapperStatsSet(pid) = mapperStats
    ret
  }

  override def equals(other: Any): Boolean = other match {
    case h: PartialKeyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
