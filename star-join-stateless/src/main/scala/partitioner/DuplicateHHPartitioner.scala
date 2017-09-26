package partitioner

import com.google.common.hash.Hashing
import org.apache.spark.Partitioner

import scala.collection.mutable

/**
  * Created by kawhi on 10/08/2017.
  */
class DuplicateHHPartitioner(partitions : Int, seed: Int) extends Partitioner{
  // 每个
  val hash = Hashing.murmur3_128(seed)
  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(Int, String)]
    if (k._1 == -1) {
      // light key: Hash
      return (Math.abs(hash.hashBytes(k._2.getBytes()).asLong()) % partitions).toInt
    } else {
      // heavy hitter: 钦定了.
      return k._1
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: DuplicateHHPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
