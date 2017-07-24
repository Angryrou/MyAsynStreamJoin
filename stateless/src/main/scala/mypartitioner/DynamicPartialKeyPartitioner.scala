package mypartitioner

import cluster.PartialKeyDynamicConfig
import com.google.common.hash.{HashFunction, Hashing}
import org.apache.spark.Partitioner

/**
  * Created by kawhi on 23/05/2017.
  */
class DynamicPartialKeyPartitioner(partitions : Int) extends Partitioner {
  private val mapperStats = new Array[Int](partitions)
//  private val seeds = PartialKeyDynamicKey.getSeeds()
//  private val hashes = seeds.map(Hashing.murmur3_128(_))
  var hashes : Array[HashFunction] = Array()

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    if (hashes.isEmpty) {
      hashes = PartialKeyDynamicConfig.getSeeds().map(Hashing.murmur3_128(_))
    }
    val choices = hashes.map(h => (Math.abs(h.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt)
    var ret = choices(0)
    choices.foreach(c => {
      if (mapperStats(ret) > mapperStats(c))
        ret = c
    })
    mapperStats(ret) += 1
    ret
  }

  override def equals(other: Any): Boolean = other match {
    case h: DynamicPartialKeyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
