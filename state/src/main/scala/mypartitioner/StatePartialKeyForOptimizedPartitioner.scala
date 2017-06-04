package mypartitioner

import com.google.common.hash.Hashing
import org.apache.spark.Partitioner

import scala.util.Try

/**
  * Created by kawhi on 03/06/2017.
  */
class StatePartialKeyForOptimizedPartitioner(partitions: Int, seeds: Array[Int]) extends Partitioner {
  private val hashes = seeds.map(Hashing.murmur3_128(_))
  private val mapperStats = new Array[Int](partitions)

  def tryToInt(s: String) = Try(s.toInt).toOption

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val triggerPort = tryToInt(key.asInstanceOf[(String, BigInt)]._1)
    if (triggerPort != None) {
      val t = triggerPort.get
      if (t >= 0 && t < partitions) {
        return t
      } else {
        Console.err.println(s"preprocessing data error: key is $key, " +
          s"transformed to $t, not in the range [0-${numPartitions-1}]")
        return 0
      }
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
    case h: StatePartialKeyForOptimizedPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}