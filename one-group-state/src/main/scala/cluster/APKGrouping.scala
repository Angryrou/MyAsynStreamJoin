package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}
import org.apache.spark.streaming.{MyStateSpecWithIndex, Seconds, State, StreamingContext}
import partitioner.{StateAPKPartitioner, StatePartialKeyForOptimizedPartitioner}
import timetable.MyStateJoinUtils
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._

import scala.collection.mutable

/**
  * Created by kawhi on 03/10/2017.
  */
object APKGrouping {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: APKGrouping_state <stream.json> 1,2 multiple_tuple")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, relation_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val multiple = Integer.parseInt(args(2)) // 把 tuple 放大到多少条
    val seeds= args(1).split(",").map(_.toInt)

    val mapperIdSet = (0 until m).map(_.toString)
    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("APKGrouping_state")
        .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")

    // Broadcast
    val myBroadcastUnfinished = BroadcastWrapper[Map[BigInt, Set[String]]](ssc, Map[BigInt, Set[String]]())
    val myBroadcastJustFinished = BroadcastWrapper[Map[BigInt, Set[String]]](ssc, Map[BigInt, Set[String]]())

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: ((z, ltw), x) or ((partitionId, ltw), x)
    //        1. signal: new time comes
    //        2. time data
    val pre = (id: Int, iter : Iterator[(String, String)]) => {

      // 两个value 的 map 一定不重复,所以可以直接 ++ 连接.否自会有覆盖.
      val etwHead = myBroadcastUnfinished.value ++ myBroadcastJustFinished.value
      println(s"loader-$id: ")
      for (eh <- etwHead) {
        println(s"etw ${eh._1}: ${eh._2.mkString(",")}")
      }
      APKMate.updateHeadTable(id, etwHead)

      val ret = mutable.ListBuffer[((String, BigInt), Int)]() // return type
      var ltwIndex:BigInt = 0
      while (iter.hasNext) {
        val tmp10 = iter.next()._2.split(";")
        for (t <- tmp10) {
          val tmp = t.split(' ')
          val ltw = BigInt(tmp(0)) / lgw
          val z = tmp(1)
//          val x = tmp(2).toInt

          // ltwIndex 表示已经读入的最大时间, ltw 表示最新读入的时间
          // ltwIndex < ltw 表示读到新时间的数据了
          // ltwIndex > ltw 乱序数据,与数据类型假设相反
          // ltwIndex == ltw 表示读到同一个lgw 的数据

          if (ltwIndex < ltw) {
            mapperIdSet.foreach(key => {
              ret += ((key, ltw - 1) -> id)
            })
            ltwIndex = ltw
          }
          for (a <- 1 to multiple) {
            ret += ((z, ltw) -> 1)
          }
        }
      }
      ret.iterator
    }

    def mappingFuncAPK(partitionId: Int, zLtw: (String, BigInt), one: Option[Int],
                      state: State[Int]):
    Option[((String, BigInt), Int)] = {

      // state 存的是当前最大值.
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
          state.update(mp + 1)
          return None
        }
      }
    }

    val localMergeE = (iter: Iterator[((String, BigInt), Int)]) => { // ((z, ltw), sum)
      val ret = mutable.Map[(String, BigInt), Int]()
      val etwSum = mutable.Map[BigInt, Int]()

      var dispersion = 0
      while (iter.hasNext) {
        val ztxl = iter.next()
        ret.get(ztxl._1) match {
          case Some(v) => {
            dispersion += 1
            ret(ztxl._1) = ztxl._2 + v
            MyUtils.sleepNanos(sleep_time_reduce_ns)
          }
          case None => ret(ztxl._1) = ztxl._2
        }
        val curEtwSum = etwSum.getOrElse(ztxl._1._2, 0)
        etwSum(ztxl._1._2) = ztxl._2 + curEtwSum
      }
      ReduceMate.dispersionE = dispersion
      ReduceMate.globalSumE = etwSum.toMap
      ret.iterator
    }

    val localMergeS = (iter: Iterator[((String, BigInt), Int)]) => { // ((z, ltw), sum)
      val ret = mutable.Map[(String, BigInt), Int]()
      val etwSum = mutable.Map[BigInt, Int]()

      var dispersion = 0
      while (iter.hasNext) {
        val ztxl = iter.next()
        ret.get(ztxl._1) match {
          case Some(v) => {
            dispersion += 1
            ret(ztxl._1) = ztxl._2 + v
            MyUtils.sleepNanos(sleep_time_reduce_ns)
          }
          case None => ret(ztxl._1) = ztxl._2
        }
        val curEtwSum = etwSum.getOrElse(ztxl._1._2, 0)
        etwSum(ztxl._1._2) = ztxl._2 + curEtwSum
      }
      ReduceMate.dispersionS = dispersion
      ReduceMate.globalSumS = etwSum.toMap
      ret.iterator
    }

    val spec_apk = MyStateSpecWithIndex.function(mappingFuncAPK _)
      .partitioner(new StateAPKPartitioner(m, seeds))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      //      .flatMap(_._2.split(";"))
      .transform(_.mapPartitionsWithIndex(pre))
      .myMapWithStateWithIndex(spec_apk, relation_num, true)

    messages.stateSnapshots().foreachRDD((rdd, time) => {
      // stateSnapShots() 类型为 (z, etw) -> stateValue
      // 获得尚未完成的地方的 key distribution
      println(s"----- stateSnapshots $time -----")
      val rdd1 = rdd.partitionBy(new HashPartitioner(r))
        .mapPartitions(localMergeS)
      val (newEtwHead, dispersionS) = rdd1.aggregate(mutable.Map[BigInt, mutable.Set[String]](), 0)(
        (init, e) => {
          // 先获得 e 的信息看看是哪个 etw 的
          val ((z, etw), zSum) = e
          // 对于 这个 etw 上的 这个 zsum 值判断是否是 heavyhitter
          if (zSum > ReduceMate.globalSumS.getOrElse(etw, 0) * 0.2 / m) {
            val a = init._1.getOrElse(etw, mutable.Set[String]())
            a.add(z)
            init._1(etw) = a
          }
          val d = ReduceMate.dispersionS
          (init._1, d)
        }
        , (init, b) => b
      )
      val newEtwH = newEtwHead.map(kv => (kv._1, kv._2.toSet)).toMap
      for (eh <- newEtwHead) {
        println(s"state: etw ${eh._1}: ${eh._2.mkString(",")}")
      }
      println(s"DispersionS: $dispersionS")
      myBroadcastUnfinished.update(newEtwH, true)
      println()
    })

    messages.foreachRDD((rdd, time) => {
      println(s"----- emitContent $time -----")
      val rdd1 = rdd.filter(!_.equals(None))
        .map(_.get)
        .partitionBy(new HashPartitioner(r))
        .mapPartitions(localMergeE)
      // (z, etw) -> stateValue
      val (newEtwHead, dispersionE) = rdd1.aggregate(mutable.Map[BigInt, mutable.Set[String]](), 0)(
        (init, e) => {
          // 先获得 e 的信息看看是哪个 etw 的
          val ((z, etw), zSum) = e
          // 对于 这个 etw 上的 这个 zsum 值判断是否是 heavyhitter
          if (zSum > ReduceMate.globalSumE.getOrElse(etw, 0) * 0.2 / m) {
            val a = init._1.getOrElse(etw, mutable.Set[String]())
            a.add(z)
            init._1(etw) = a
          }
          val d = ReduceMate.dispersionE
          (init._1, d)
        }
        , (init, b) => b
      )
      val newEtwH = newEtwHead.map(kv => (kv._1, kv._2.toSet)).toMap
      for (eh <- newEtwHead) {
        println(s"emit: etw ${eh._1}: ${eh._2.mkString(",")}")
      }
      println(s"DispersionE: $dispersionE")
      myBroadcastJustFinished.update(newEtwH, true)
      println()
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
