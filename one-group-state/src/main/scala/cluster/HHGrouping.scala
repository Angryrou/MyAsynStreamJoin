package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.{HHPartitioner, StateHashForOptimizedPartitioner}
import org.apache.spark.streaming.{MyStateSpecWithIndex, Seconds, State, StreamingContext}
import timetable.MyStateJoinUtils
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._

import scala.collection.mutable

/**
  * Created by kawhi on 06/09/2017.
  */
object HHGrouping {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: HHGrouping_state <stream.json> hash_seed multiple_tuple")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, relation_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val seed = Integer.parseInt(args(1))
    val multiple = Integer.parseInt(args(2)) // 把 tuple 放大到多少条

    val mapperIdSet = (0 until m).map(_.toString)

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("HHGrouping_state")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: ((z, ltw), x) or ((partitionId, ltw), x)
    //        1. signal: new time comes
    //        2. time data
    val preProcess = (iter : Iterator[String]) => {
      val pid = TaskContext.getPartitionId()
      val ret = mutable.ListBuffer[((String, BigInt), Int)]() // return type
      var ltwIndex:BigInt = 0
      while (iter.hasNext) {
        val tmp = iter.next().split(' ')
        val ltw = BigInt(tmp(0)) / lgw
        val z = tmp(1)
        val x = tmp(2).toInt

        // ltwIndex 表示已经读入的最大时间, ltw 表示最新读入的时间
        // ltwIndex < ltw 表示读到新时间的数据了
        // ltwIndex > ltw 乱序数据,与数据类型假设相反
        // ltwIndex == ltw 表示读到同一个lgw 的数据

        if (ltwIndex < ltw) {
          mapperIdSet.foreach(key => {
            ret += ((key, ltw - 1) -> pid)
          })
          ltwIndex = ltw
        }
        for (a <- 1 to multiple) {
          ret += ((z, ltw) -> x)
        }
      }
      ret.iterator
    }

    def mappingFuncHH(partitionId: Int, zLtw: (String, BigInt), one: Option[Int],
                      state: State[Int]):
    Option[((String, BigInt), Int)] = {

      // state 存的是当前Sum.
//      val mp = state.getOption().getOrElse(new mutable.ArrayBuffer[Int])
      val mp = state.getOption().getOrElse(0)
      one match {
        case None => {
          // 说明是 trigger 时间信号已经在 StateJoinUtils 里刚刚产生
          // 而且, 该 key 所存的数据的 logical-time-window 的时间 <= trigger (由 MyMapWithStateWithIndexRDD 保证)
          // state 的数据已经可以 emit 并删除
          val trigger = MyStateJoinUtils.getPartitionTriggers(partitionId)
          val ret = ((zLtw._1, zLtw._2), mp)
          state.remove()
          return Some(ret)
        }
        case Some(p) => {
          // 说明是正常数据加入,emitted 数据是 None
//          mp = Math.max(mp, p)
          MyUtils.sleepNanos(sleep_time_map_ns)
          state.update(mp+1)
          return None
        }
      }
    }

    val spec_hh = MyStateSpecWithIndex.function(mappingFuncHH _).partitioner(new StateHashForOptimizedPartitioner(m, seed))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .mapPartitions(preProcess)
      .myMapWithStateWithIndex(spec_hh, relation_num, true)
//      .checkpoint(Seconds(batch_duration))

    val res = messages
      .filter(!_.equals(None))
      .map(_.get)

    res.foreachRDD((rdd, time) => {
      rdd.foreach(println)
      println(s"----- $time -----")
      println()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
