package mypartitioner

import java.util.Random

import org.apache.spark.Partitioner

import scala.util.Try

/**
  * Created by kawhi on 03/06/2017.
  */
class StateRoundRobinForOptimizedPartitioner(partitions: Int) extends Partitioner {
  private var p: Int = new Random().nextInt(partitions)

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

    if (p == numPartitions - 1) {
      p = 0
    } else {
      p += 1
    }
    return p

  }

  override def equals(other: Any): Boolean = other match {
    case h: StateRoundRobinForOptimizedPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
