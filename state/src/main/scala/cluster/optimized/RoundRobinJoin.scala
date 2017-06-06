package cluster.optimized

import kafka.serializer.StringDecoder
import mypartitioner.StateRoundRobinForOptimizedPartitioner
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{MyStateSpecWithIndex, Seconds, State, StreamingContext}
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._
import timetable.MyStateJoinUtils

import scala.collection.mutable
/**
  * Created by kawhi on 04/06/2017.
  */

object RoundRobinJoin {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: RoundRobinJoin <stream.json>")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val mapperIdSet = (0 until m).map(_.toString)
    val isOptimized = true

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("RoundRobinJoin_state_optimized")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // element:"timestamp port word" => ((word, ltw), port) or ((partitionId, ltw), port)
    val preProcessing = (iter: Iterator[String]) => {
      val ret = mutable.ListBuffer[((String, BigInt), Int)]()
      val portMap = mutable.Map[Int, BigInt]() // 当前每个 port 到达 ltw 的最大值
      // 同一个 port 的数据都在同一个 iter 内部
      while (iter.hasNext) {
        val tmp = iter.next().split(' ') // "timestamp port word"
        val ltw = BigInt(tmp(0)) / lgw
        val tw = tmp(0)
        val port = tmp(1).toInt
        val word = tmp(2)
        // 若 portMap(port) 中已经含有 ltw 的信息, 正常读入; 否则 告诉所有 mappers (ltw - 1) 前的数据都到了
        // 注意到, 每个 batch 开始的时候, 接收到的第一个时间戳 也会向各个 key 进行报告.
        // 虽然同一个时间被报告可能至少一次, 但是解决了 ltw 恰好被 system-time 划分的问题
        if (portMap.getOrElse(port, 0: BigInt) < ltw) {
          // 发送给所有 mapper, 这个 port 上, 所有小于等于 (ltw - 1) 的数据都到了
          mapperIdSet.foreach(key => {
            ret += ((key, ltw - 1) -> port) // 用 t 表示 小于等于 t 的时间的数据都已经到了
          })
          portMap(port) = ltw
        }
        ret += ((word, ltw) -> port)
      }
      ret.iterator
    }

    def mappingFuncRR(partitionId: Int, wordLtw: (String, BigInt), one: Option[Int],
                      state: State[mutable.Map[Int, Int]]):
    Option[((String, BigInt), mutable.Map[Int, Int])] = {  // (word, lgw), Map{port -> local_count}
      // state 存的 map 是一个 {port -> count} 的东西
      val mp = state.getOption().getOrElse(mutable.Map[Int, Int]())

      one match {
        case None => {
          // 说明是 trigger 时间信号已经在 StateJoinUtils 里刚刚产生
          // 而且, 该 key 所存的数据的 logical-time-window 的时间 <= trigger (由 MyMapWithStateWithIndexRDD 保证)
          // state 的数据已经可以 emit 并删除
          val trigger = MyStateJoinUtils.getPartitionTriggers(partitionId)
          val ret = (wordLtw, mp)
          state.remove()
          return Some(ret)
        }
        case Some(p) => {
          // 说明是正常数据加入,emitted 数据是 None
          MyUtils.sleepNanos(sleep_time_map_ns)
          mp(p) = mp.getOrElse(p, 0) + 1
          state.update(mp)
          return None
        }
      }
    }

    val globalMerge = (iter: Iterator[((String, BigInt), mutable.Map[Int, Int])]) => {
      // (word, ltw), {port -> local_count} ==> (word, ltw), {port -> global_count}
      val ret = mutable.Map[(String, BigInt), mutable.Map[Int, Int]]()
      while (iter.hasNext) {
        val wtpc = iter.next() // ((word, ltw), {port -> local_count})
        val pcLocalMap = wtpc._2
        val pcGlobalMap = ret.getOrElse(wtpc._1, mutable.Map[Int, Int]()) // {port -> global_count}
        pcLocalMap.foreach { case (port, local_count) =>
          MyUtils.sleepNanos(sleep_time_reduce_ns)
          val global_count = pcGlobalMap.getOrElse(port, 0) + local_count
          pcGlobalMap(port) = global_count
        }
        ret(wtpc._1) = pcGlobalMap
      }
      // (word, ltw), {port -> global_count} ==> (word, ltw), min_count
      val res = ret.map(wtpc => {
        (wtpc._2.size == ports_num) match {
          case true =>
            (wtpc._1._1, (wtpc._1._2, wtpc._2.values.min))
          case _ =>
            (wtpc._1._1, (wtpc._1._2, 0))
        }
      })
      res.iterator // (word, (lgw, min_count))
    }

    val spec_rr = MyStateSpecWithIndex.function(mappingFuncRR _)
      .partitioner(new StateRoundRobinForOptimizedPartitioner(m))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .mapPartitions(preProcessing)
      .myMapWithStateWithIndex(spec_rr, ports_num, isOptimized)
      .filter(!_.equals(None))
      .map(_.get)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(globalMerge)

    messages.foreachRDD((rdd, time) => {
      println(s"----- $time -----")
      rdd.mapPartitionsWithIndex((index, iter) => {
        iter.foreach(ele => {
          println(s"partition: $index\t$ele")
        })
        iter
      }).collect
    })
    ssc.start()
    ssc.awaitTermination()
  }
}