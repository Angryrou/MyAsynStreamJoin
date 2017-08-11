package partitioner

import com.google.common.hash.Hashing
import org.apache.spark.Partitioner

import scala.collection.mutable

/**
  * Created by kawhi on 10/08/2017.
  */
class HHPartitioner(partitions : Int, seed: Int) extends Partitioner{
  // 每个
  private val mapperStatsSet = mutable.Map[Int, Array[Int]]() // 每个 executor 中的每个 partition 都有一个维护自己的 Array

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    val hash = Hashing.murmur3_128(seed)
    val choice = (Math.abs(hash.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt
    return choice
  }

  override def equals(other: Any): Boolean = other match {
    case h: HHPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
