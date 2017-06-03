package mypartitioner

import com.google.common.hash.Hashing
import org.apache.spark.Partitioner

/**
  * Created by kawhi on 23/05/2017.
  */
class PartialKeyPartitioner(partitions : Int, seeds : Array[Int]) extends Partitioner {
  private val hashes = seeds.map(Hashing.murmur3_128(_))
  private val mapperStats = new Array[Int](partitions)

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
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
    case h: PartialKeyPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
