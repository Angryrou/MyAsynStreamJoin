package partitioner

import cluster.APKConfig
import com.google.common.hash.Hashing
import org.apache.spark.{Partitioner, TaskContext}

import scala.collection.mutable

/**
  * Created by kawhi on 09/08/2017.
  */
class APKPartitioner(partitions : Int) extends Partitioner{

  // 每个 executor 都会 new 一个该类,所以该类的内容会被多个partition用到.
  private val tailSeeds = Array[Int](1,2)
  private val tailHashes = tailSeeds.map(Hashing.murmur3_128(_))
//  private val headHashes = seeds.map(Hashing.murmur3_128(_))
  private var head = Set[String]()
  private val mapperStatsSet = mutable.Map[Int, Array[Int]]() // 每个 executor 中的每个 partition 都有一个维护自己的 Array
  private var pid = -1

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    // 先确定这个 key 来自哪个 partitioner 并把这个 Partitioner 的 hashes 和 mapperStats 都取到
    if (pid != TaskContext.getPartitionId()){
      pid = TaskContext.getPartitionId()
      head = APKConfig.getHead(pid)
    }
    val mapperStats = mapperStatsSet.getOrElse(pid, new Array[Int](partitions))

    // 做多个choice 并且做出选择
    val skey = key.toString
    val choices = if (head.contains(skey)) {
//      headHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
      (0 until partitions).toArray
    } else {
      tailHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
    }

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
    case h: APKPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
