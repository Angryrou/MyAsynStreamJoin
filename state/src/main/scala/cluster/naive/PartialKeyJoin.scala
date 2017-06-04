package cluster.naive



import kafka.serializer.StringDecoder
import mypartitioner.StatePartialKeyPartitioner
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{MyStateSpecWithIndex, Seconds, State, StreamingContext}
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._
import timetable.MyStateJoinUtils

import scala.collection.mutable

/**
  * Created by kawhi on 03/06/2017.
  */
object PartialKeyJoin {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: PartialKeyJoin <stream.json> 13,19")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_ns)
    = MyUtils.getFromJson(args(0))
    val seeds= args(1).split(",").map(_.toInt)
    val mapperIdSet = (0 until m).map(_.toString)

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("PartialKeyJoin_state")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // element:"timestamp port word" => (word, (port, ltw))
    val preProcessing = (iter: Iterator[String]) => {
      val ret = mutable.ListBuffer[(String, (Int, BigInt))]()
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
            ret += (key ->(port, ltw - 1)) // 用 t 表示 小于等于 t 的时间的数据都已经到了
          })
          portMap(port) = ltw
        }
        ret += (word ->(port, ltw))
      }
      ret.iterator
    }

    def mappingFuncPK(partitionId: Int, word: String, one: Option[(Int, BigInt)],
                      state: State[mutable.Map[BigInt, mutable.Map[Int, Int]]]):
    Option[mutable.ArrayBuffer[((String, BigInt), mutable.Map[Int, Int])]] = {
      // state 存的 map 是一个 {lgw -> {port -> count}} 的东西
      val mp = state.getOption().getOrElse(mutable.Map[BigInt, mutable.Map[Int, Int]]())

      one match {
        case None => {
          // 说明是 trigger 时间信号已经在 StateJoinUtils 里刚刚产生
          // trigger 时间之前的数据已经可以 emit 并删除
          val trigger = MyStateJoinUtils.getPartitionTriggers(partitionId)
          val ret = mutable.ArrayBuffer[((String, BigInt), mutable.Map[Int, Int])]()

          // 将 trigger 时间之前的数据拎出 pcMap
          mp.foreach { case (lgw, pcMap) =>
            if (lgw <= trigger) {
              ret.append(((word, lgw), pcMap))
            }
          }

          // 将该 key 的 state 里的所有 trigger 之前的时间的数据删除
          ret.foreach { case ((_, lgw), _) =>
            mp.remove(lgw)
          }

          // 如果该 key 的 state 里的所有数据都已经被清空,那么干脆把这个 state 清空了,不然就更新一下state
          if (mp.isEmpty) {
            state.remove()
          } else {
            state.update(mp)
          }
          if (ret.isEmpty)
            return None
          else
            return Some(ret)
        }
        case Some(pt) => {
          // 说明是正常数据加入,emitted 数据是 None
          val pcMap = mp.getOrElse(pt._2, mutable.Map[Int, Int]()) // {port -> count
          pcMap(pt._1) = pcMap.getOrElse(pt._1, 0) + 1
          MyUtils.sleepNanos(sleep_time_ns)
          mp(pt._2) = pcMap
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
          MyUtils.sleepNanos(sleep_time_ns)
          val global_count = pcGlobalMap.getOrElse(port, 0) + local_count
          pcGlobalMap(port) = global_count
        }
        ret(wtpc._1) = pcGlobalMap
      }
      val res = ret.map(wtpc => {
        (wtpc._2.size == ports_num) match {
          case true =>
            (wtpc._1._1, (wtpc._1._2, wtpc._2.values.min))
          case _ =>
            (wtpc._1._1, (wtpc._1._2, 0))
        }
      })
      res.iterator
    }

    val spec_rr = MyStateSpecWithIndex.function(mappingFuncPK _).partitioner(new StatePartialKeyPartitioner(m, seeds))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .mapPartitions(preProcessing)
      .myMapWithStateWithIndex(spec_rr, ports_num)
      .filter(!_.equals(None))
      .flatMap(_.get)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(globalMerge)


    messages.foreachRDD(_.collect)
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
