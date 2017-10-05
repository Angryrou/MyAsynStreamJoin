package partitioner

/**
  * Created by kawhi on 05/10/2017.
  */

import cluster.{APKMate, DMate}
import com.google.common.hash.Hashing
import org.apache.spark.{Partitioner, TaskContext}

import scala.collection.mutable
import scala.util.Try

class StateDPartitioner(partitions : Int, seeds : Array[Int]) extends Partitioner{

  // 每个 executor 都会 new 一个该类,所以该类的内容会被多个partition用到.
  private val tailSeeds = seeds
  private val hash = Hashing.murmur3_128(seeds(0))
  private val tailHashes = tailSeeds.map(Hashing.murmur3_128(_))
  //  private val headHashes = seeds.map(Hashing.murmur3_128(_))
  private var etwHead = Map[BigInt, Set[String]]()
  private var etwStrategy = Map[BigInt, Int]()
  private val mapperStatsSet = mutable.Map[Int, Array[Int]]() // 每个 executor 中的每个 partition 都有一个维护自己的 Array
  private var pid = -1
  def tryToInt(s: String) = Try(s.toInt).toOption

  def numPartitions : Int = partitions

  def getPartition(key: Any): Int = {

    // 先确定这个 key 来自哪个 partitioner 并把这个 Partitioner 的 hashes 和 mapperStats 都取到
    if (pid != TaskContext.getPartitionId()){
      pid = TaskContext.getPartitionId()
      etwHead = DMate.getAllHead(pid)
      etwStrategy = DMate.getAllStrategy(pid)
      println("---- print head ----")
      for (eh <- etwHead) {
        println(s"etw ${eh._1}: ${eh._2.mkString(",")}")
      }
    }
    val mapperStats = mapperStatsSet.getOrElse(pid, new Array[Int](partitions))

    // 先拿到 key 里包裹的 时间/z 的信息
    val (z, etw) = key.asInstanceOf[(String, BigInt)]

    // 如果是 trigger 类的 msgs, 直接按照要求分发
    val downloadStreamPartitionId = tryToInt(z)
    if (downloadStreamPartitionId != None) {
      val t = downloadStreamPartitionId.get
      if (t >= 0 && t < partitions) {
        t
      } else {
        Console.err.println(s"preprocessing data error: key is $key, " +
          s"transformed to $t, not in the range [0-${numPartitions-1}]")
        -1
      }
    } else {
      // 否则, 根据 etw 对应的策略调整
      val strategyId = etwStrategy.getOrElse(etw, 0)
      strategyId match {
        case 0 => {
          // hash
          val choice = (Math.abs(hash.hashBytes(key.toString.getBytes()).asLong()) % partitions).toInt
          choice
        }
        case 1 => {
          // apk
          val skey = key.toString
          val choices = if (etwHead.getOrElse(etw, Set[String]()).contains(z)) {
            //      headHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
            (0 until partitions).toArray
          } else {
            tailHashes.map(h => (Math.abs(h.hashBytes(skey.getBytes()).asLong()) % partitions).toInt)
          }
          var ret = choices(0)
          choices.foreach(c => {
            if (mapperStats(ret) > mapperStats(c))
              ret = c
          })
          mapperStats(ret) += 1
          // 对选择进行记录和更新
          mapperStatsSet(pid) = mapperStats
          ret
        }
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: StateDPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
