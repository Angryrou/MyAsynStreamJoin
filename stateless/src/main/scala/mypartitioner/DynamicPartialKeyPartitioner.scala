package mypartitioner

import cluster.PartialKeyDynamicConfig
import com.google.common.hash.{HashFunction, Hashing}
import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by kawhi on 23/05/2017.
  */
class DynamicPartialKeyPartitioner(partitions : Int) extends Partitioner {
  private val mapperStatsSet = mutable.Map[Int, Array[Int]]() // 每个 executor 中的每个 partition 都有一个维护自己的 Array
//  private val mapperStats = new Array[Int](partitions)
//  private val seeds = PartialKeyDynamicKey.getSeeds()
//  private val hashes = seeds.map(Hashing.murmur3_128(_))
  var hashes : Array[HashFunction] = Array()
  var pid : Int = -1

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    // 先确定这个 key 来自哪个 partitioner 并把这个 Partitioner 的 hashes 和 mapperStats 都取到
    if (pid != TaskContext.getPartitionId()) {
      pid = TaskContext.getPartitionId()
      hashes = PartialKeyDynamicConfig.getSeeds(pid).map(Hashing.murmur3_128(_))
    }
    val mapperStats = mapperStatsSet.getOrElse(pid, new Array[Int](partitions))

    // 做多个choice 并且做出选择
    val choices = hashes.map(h => (Math.abs(h.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt)
    var ret = choices(0)
    choices.foreach(c => {
      if (mapperStats(ret) > mapperStats(c))
        ret = c
    })
    mapperStats(ret) += 1

    // 对选择进行记录和更新
    mapperStatsSet(pid) = mapperStats
    ret
  }

  override def equals(other: Any): Boolean = other match {
    case h: DynamicPartialKeyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
