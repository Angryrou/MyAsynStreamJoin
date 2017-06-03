package mypartitioner

import myutils.MyUtils
import org.apache.spark.Partitioner

import scala.util.Try

/**
  * Created by kawhi on 03/06/2017.
  */
class StateHashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def tryToInt(s: String) = Try(s.toInt).toOption

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {

    val triggerPort = tryToInt(key.asInstanceOf[String])
    if (triggerPort != None) {
      val t = triggerPort.get
      if (t >= 0 && t < partitions) {
        return t
      } else {
        Console.err.println(s"preprocessing data error: key is $key, " +
          s"transformed to $t, not in the range [0-${numPartitions - 1}]")
        return 0
      }
    }

    return key match {
      case null => 0
      case _ => MyUtils.nonNegativeMod(key.hashCode, numPartitions)
    }
  }


  override def equals(other: Any): Boolean = other match {
    case h: StateHashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}