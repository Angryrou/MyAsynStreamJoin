package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{MyStateSpecWithIndex, Seconds, State, StreamingContext}
import partitioner.{HHPartitioner, NewStatePartialKeyForOptimizedPartitioner, StateHashForOptimizedPartitioner, StatePartialKeyForOptimizedPartitioner}
import timetable.MyStateJoinUtils

import scala.collection.mutable
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._

/**
  * Created by kawhi on 07/09/2017.
  */
object Test {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: TestHHGrouping_state <stream.json> multiple")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, relation_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))

//    val seeds= args(1).split(",").map(_.toInt)
    val mapperIdSet = (0 until m).map(_.toString)
    val multiple = Integer.parseInt(args(1))
//    val stateLength = Integer.parseInt(args(2))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("TestHHGrouping_state")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    val mapLocalCompute = (iter : Iterator[(String, Int)]) => {
      val ret = mutable.Map[String, Int]()
      while (iter.hasNext) {
        val w = iter.next() // (word, 1)
        ret(w._1) = ret.getOrElse(w._1, 0) + 1
        //        MyUtils.sleepNanos(sleep_time_map_ns)
      }
      ret.iterator
    }

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: ((z, ltw), x) or ((partitionId, ltw), x)
    //        1. signal: new time comes
    //        2. time data
    val preProcess = (iter : Iterator[(String, String)]) => {
      val ret = mutable.ArrayBuffer[((String, BigInt), String)]() // return type
      while (iter.hasNext) {
        val a = iter.next()
        ret.append(((a._1, BigInt(0)), a._2))
      }
      for (i <- 1 to multiple) {
        ret ++= ret
      }

//      var ltwIndex:BigInt = 0
//      while (iter.hasNext) {
//        val tmp = iter.next().split(' ')
//        val ltw = BigInt(tmp(0)) / lgw
//        val z = tmp(1)
//        val x = tmp(2).toInt
//
//        // ltwIndex 表示已经读入的最大时间, ltw 表示最新读入的时间
//        // ltwIndex < ltw 表示读到新时间的数据了
//        // ltwIndex > ltw 乱序数据,与数据类型假设相反
//        // ltwIndex == ltw 表示读到同一个lgw 的数据
//
//        if (ltwIndex < ltw) {
//          mapperIdSet.foreach(key => {
//            ret += ((key, ltw - 1) -> x)
//          })
//          ltwIndex = ltw
//        }
//        for (a <- 1 to multiple) {
//          ret += ((z, ltw) -> x)
//        }
//      }
      ret.iterator
    }

    def mappingFuncHH(partitionId: Int, zLtw: (String, BigInt), one: Option[String],
                      state: State[mutable.ListBuffer[String]]):
    Option[((String, BigInt), Int)] = {

      // state 存的是当前最大值.
      val mp = state.getOption().getOrElse(new mutable.ListBuffer[String])

      one match {
        case None => {
          // 说明是 trigger 时间信号已经在 StateJoinUtils 里刚刚产生
          // 而且, 该 key 所存的数据的 logical-time-window 的时间 <= trigger (由 MyMapWithStateWithIndexRDD 保证)
          // state 的数据已经可以 emit 并删除
          val trigger = MyStateJoinUtils.getPartitionTriggers(partitionId)
          val ret = ((zLtw._1, zLtw._2), mp.length)
          state.remove()
          return Some(ret)
        }
        case Some(p) => {
          // 说明是正常数据加入,emitted 数据是 None
//                    mp = Math.max(mp, p)
//          if (mp.length < stateLength) {
          mp.append(p)
//          }
          MyUtils.sleepNanos(sleep_time_map_ns)

          state.update(mp)
          return None
        }
      }
    }

    val spec_hh = MyStateSpecWithIndex.function(mappingFuncHH _).partitioner(new StateHashForOptimizedPartitioner(m, 1))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
//      .map(e => ((e._1, BigInt(0)), e._2))
      .mapPartitions(preProcess)
      .myMapWithStateWithIndex(spec_hh, relation_num, true)
      .checkpoint(Seconds(1000 * batch_duration))
    messages.print()
    messages.foreachRDD((rdd, time) => {
      rdd.foreachPartition(iter => {
        var i = 0
        while(iter.hasNext){
          iter.next()
          i += 1
        }
        println(s"$time, count: $i")
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
