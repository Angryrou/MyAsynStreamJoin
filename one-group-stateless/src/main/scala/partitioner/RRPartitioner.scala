package partitioner

import java.util.Random

import org.apache.spark.Partitioner

/**
  * Created by kawhi on 09/08/2017.
  */
class RRPartitioner(partitions : Int) extends Partitioner{
  private var p : Int = new Random().nextInt(partitions)
  // private var a = new Random()

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    if (p == numPartitions - 1) {
      p = 0
    } else{
      p += 1
    }
    p
    //    a.nextInt(partitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: RRPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
