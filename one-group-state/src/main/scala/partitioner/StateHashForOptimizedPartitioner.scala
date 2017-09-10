package partitioner

import com.google.common.hash.Hashing
import myutils.MyUtils
import org.apache.spark.Partitioner

import scala.util.Try

/**
  * Created by kawhi on 03/06/2017.
  */
class StateHashForOptimizedPartitioner(partitions: Int, seed: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def tryToInt(s: String) = Try(s.toInt).toOption
  val hash = Hashing.murmur3_128(seed)

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val downStreamPartitionId = tryToInt(key.asInstanceOf[(String, BigInt)]._1)
    if (downStreamPartitionId != None) {
      val t = downStreamPartitionId.get
      if (t >= 0 && t < partitions) {
        return t
      } else {
        Console.err.println(s"preprocessing data error: key is $key, " +
          s"transformed to $t, not in the range [0-${numPartitions - 1}]")
        return 0
      }
    }
    return (Math.abs(hash.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt
  }


  override def equals(other: Any): Boolean = other match {
    case h: StateHashForOptimizedPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}