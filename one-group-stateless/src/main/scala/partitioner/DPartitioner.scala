package partitioner

import java.util.Random

import cluster.DMate
import com.google.common.hash.Hashing
import org.apache.spark.{Partitioner, TaskContext}

import scala.collection.mutable

/**
  * Created by kawhi on 09/08/2017.
  */
class DPartitioner(partitions: Int, seeds: Array[Int]) extends Partitioner {

  // For APK
  // 每个 executor 都会 new 一个该类,所以该类的内容会被多个partition用到.
  private val tailSeeds = seeds
  private val tailHashes = tailSeeds.map(Hashing.murmur3_128(_))
  //  private val headHashes = seeds.map(Hashing.murmur3_128(_))
  private var head = Set[String]()
  private val mapperStatsSet = mutable.Map[Int, Array[Int]]() // 每个 executor 中的每个 partition 都有一个维护自己的 Array
  private var pid = -1

  // for Hash
  private val hash = Hashing.murmur3_128(seeds(0))

  // for Dynamic
  private var strategyId = 0

  private def getStrategy(M:Int, K:Int, m:Int, p1: Double, lambda: Double, headNum: Int):Int = {
    //  最坏情况
    //    val costHH = (1 * 0.6 + delta * 0.4) * M / m
    //  zipf m = 15
    val costHH = (13.26 * p1 + 1.02) * M / m
    // zipf m = 45
    //    val costHH = (1.3 + 46.4 * p1) * M / m
    // zipf m = 135
//    val costHH = (127.16 * p1 + 1.63) * M/m
    // zipf m = 5
//    val costHH = (2.68 * p1 + 1) * M/m
    // zipf m = 75
//    val costHH = (78.46 * p1 + 1) * M/m
    val costAPK = M/m + lambda * (K + headNum * (m - 2))

    // 0 for HH, 1 for APK
    val ret = if (costHH <= costAPK) 0 else 1
    println(s"p1: $p1,  costHH: $costHH, costAPK: $costAPK, strategy: $ret")
    ret
  }

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
      //
    if (pid != TaskContext.getPartitionId()) {
      pid = TaskContext.getPartitionId()
      head = DMate.getHead(pid)
      strategyId = getStrategy(DMate.M, DMate.K, DMate.m, DMate.p1, DMate.lambda, head.size)
      println(s"---- print head ----, using strategy: $strategyId")
      println(head.mkString(","))
    }
    strategyId match {
      case 0 => { // hh
        val choice = (Math.abs(hash.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt
        return choice
      }
      case 1 => { // apk
        val mapperStats = mapperStatsSet.getOrElse(pid, new Array[Int](partitions))

        // 做出多个 choice
        val skey = key.toString
        val choices = if (head.contains(skey)) (0 until partitions).toArray else {
          tailHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
        }

        var ret = choices(0)
        choices.foreach(c => {
          if (mapperStats(ret) > mapperStats(c))
            ret = c
        })
        mapperStats(ret) += 1
        mapperStatsSet(pid) = mapperStats
        return ret
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: DPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
