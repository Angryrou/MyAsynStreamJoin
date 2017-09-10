package cluster

import java.util.Random

import com.google.common.hash.Hashing
import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import timetable.MyStateJoinUtils
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._
import partitioner.StateDynamicForOptimizedPartitioner

import scala.collection.mutable
import scala.util.Try

/**
  * Created by kawhi on 08/09/2017.
  */
object DynamicGrouping {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: DynamicGrouping_state <stream.json>")
      System.exit(1)
    }
    val (brokers, topics, batch_duration, relation_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val mapperIdSet = (0 until m).map(_.toString)

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("DynamicGrouping_state")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")
    val myBroadcast = BroadcastWrapper[Double](ssc, 0.0)
    val otherValue = ssc.sparkContext.broadcast(Map("sleep_time_map_ns" -> sleep_time_map_ns,
      "sleep_time_reduce_ns" -> sleep_time_reduce_ns))

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    def mappingFuncPK(partitionId: Int, zLtw: (String, BigInt), one: Option[Int],
                      state: State[Int]):
    Option[((String, BigInt), Int)] = {

      // state 存的是当前最大值.
      var mp = state.getOption().getOrElse(0)
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
          mp = Math.max(mp, p)
          MyUtils.sleepNanos(sleep_time_map_ns)

          state.update(mp)
          return None
        }
      }
    }

    val localMerge = (iter: Iterator[((String, BigInt), Int)]) => { // ((z, ltw), x_local_maximum)
    val ret = mutable.Map[(String, BigInt), Int]()
      while (iter.hasNext) {
        val ztxl = iter.next()
        val curMax = ret.getOrElse(ztxl._1, 0)
        ret(ztxl._1) = Math.max(ztxl._2, curMax)
        MyUtils.sleepNanos(sleep_time_reduce_ns)
      }
      ret.iterator
    }

    val spec_pk = MyStateSpecWithIndex.function(mappingFuncPK _)
      .partitioner(new StateDynamicForOptimizedPartitioner(m))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .transform((rdd, time) => {
          rdd.mapPartitions((iter : Iterator[String]) => {
            DynamicMale.currentRate = myBroadcast.value
            println(s" $time --> ${DynamicMale.currentRate}")
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
              } else {
                ret += ((z, ltw) -> x)
              }
            }
            ret.iterator
          })
        })
      .cache()

    val res = messages
      .myMapWithStateWithIndex(spec_pk, relation_num, true)
      .filter(!_.equals(None))
      .map(_.get)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(localMerge)

    res.foreachRDD((rdd, time) => {
      rdd.foreach(println)
      println(s"----- $time -----")
      println()
    })

    def tryToInt(s: String) = Try(s.toInt).toOption
    val zRdd = messages.filter(e => tryToInt(e._1._1) == None).map(e => (e._1._1, 1))
    zRdd.foreachRDD((rdd, time) => {
      val a = rdd.reduceByKey(_ + _, 12).map(_._2)
      val newRate = if (a.count() == 0) {
        0.0
      } else {
        a.max() * 1.0 / a.sum()
      }
      myBroadcast.update(newRate, true)
      println(s" $time ==> $newRate")
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
