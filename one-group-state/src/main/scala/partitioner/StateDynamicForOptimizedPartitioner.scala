package partitioner

import java.util.Random

import cluster.DynamicMale
import com.google.common.hash.Hashing
import org.apache.spark.{Partitioner, SparkContext}

import scala.util.Try

/**
  * Created by kawhi on 03/06/2017.
  */
class StateDynamicForOptimizedPartitioner(partitions: Int) extends Partitioner {
    private val hashes = List(1,2).map(Hashing.murmur3_128(_))
    private val mapperStats = new Array[Int](partitions)
  private var p: Int = new Random().nextInt(partitions)

  def tryToInt(s: String) = Try(s.toInt).toOption
  val hash = Hashing.murmur3_128(1)
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val downloadStreamPartitionId = tryToInt(key.asInstanceOf[(String, BigInt)]._1)
    if (downloadStreamPartitionId != None) {
      val t = downloadStreamPartitionId.get
      if (t >= 0 && t < partitions) {
        return t
      } else {
        Console.err.println(s"preprocessing data error: key is $key, " +
          s"transformed to $t, not in the range [0-${numPartitions-1}]")
        return 0
      }
    }

    if(DynamicMale.currentRate > 0.1) {
      // round robin
      if (p == numPartitions - 1) {
        p = 0
      } else {
        p += 1
      }
      return p
    } else {
//      return (Math.abs(hash.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt
      // partial key
      val choices = hashes.map(h => (Math.abs(h.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt)
      var ret = choices(0)
      choices.foreach(c => {
        if (mapperStats(ret) > mapperStats(c))
          ret = c
      })
      mapperStats(ret) += 1
      ret
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: StateDynamicForOptimizedPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}