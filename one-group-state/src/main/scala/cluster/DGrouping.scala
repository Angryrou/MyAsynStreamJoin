package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.streaming.dstream.MyPairDStreamFunctions._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{MyStateSpecWithIndex, Seconds, State, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import partitioner.{StateAPKPartitioner, StateDPartitioner}
import timetable.MyStateJoinUtils

import scala.collection.mutable

/**
  * Created by kawhi on 03/10/2017.
  */
object DGrouping {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: DGrouping_state <stream.json> 1,2 multiple_tuple lambda")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, relation_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val multiple = Integer.parseInt(args(2)) // 把 tuple 放大到多少条
    val seeds= args(1).split(",").map(_.toInt)
    val lambda = args(3).toDouble

    val mapperIdSet = (0 until m).map(_.toString)
    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("DKGrouping_state")
        .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))
    ssc.checkpoint(path + "/state/checkpoint")

    // Broadcast {etw -> (strategyId, M, K, p1, H)}
    val myBroadcastUnfinished = BroadcastWrapper[Map[BigInt, (Int, Int, Int, Double, Set[String])]](
      ssc, Map[BigInt, (Int, Int, Int, Double, Set[String])]())
    val myBroadcastJustFinished = BroadcastWrapper[Map[BigInt, (Int, Int, Int, Double, Set[String])]](
      ssc, Map[BigInt, (Int, Int, Int, Double, Set[String])]())

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    def getStrategy(M:Int, K:Int, m:Int, p1: Double, lambda: Double, headNum: Int):Int = {
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

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: ((z, ltw), x) or ((partitionId, ltw), x)
    //        1. signal: new time comes
    //        2. time data
    val pre = (id: Int, iter : Iterator[(String, String)]) => {
      // 这里只需要对所有新来的 etw 进行策略的分配

      // 两个value 的 map 一定不重复,所以可以直接 ++ 连接.否自会有覆盖.
      val etwInfo = mutable.Map() ++ myBroadcastUnfinished.value ++ myBroadcastJustFinished.value
      println(s"loader-$id: ")

//      DMate.updateHeadTable(id, etwHead)

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
            if (!etwInfo.contains(ltw)) {
              if (etwInfo.isEmpty) {
                etwInfo(ltw) = (0, 150000, 3000, 0.0, Set[String]())
              } else {
                val estimateEtw = etwInfo.keys.max
                val (_, me, ke, p1e, he) = etwInfo(estimateEtw)
                val strategyIde = getStrategy(me, ke, m, p1e, lambda, he.size)
                etwInfo(ltw) = (strategyIde, me, ke, p1e, he)
              }
            }
          }
          for (a <- 1 to multiple) {
            ret += ((z, ltw) -> 1)
          }
        }
      }
      DMate.updateInfoTable(id, etwInfo.toMap.map(kv => (kv._1, (kv._2._1, kv._2._5))))
      ret.iterator
    }

    def mappingFuncD(partitionId: Int, zLtw: (String, BigInt), one: Option[Int],
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

    val spec_d = MyStateSpecWithIndex.function(mappingFuncD _)
      .partitioner(new StateDPartitioner(m, seeds))

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      //      .flatMap(_._2.split(";"))
      .transform(_.mapPartitionsWithIndex(pre))
      .myMapWithStateWithIndex(spec_d, relation_num, true)

    messages.stateSnapshots().foreachRDD((rdd, time) => {
      // stateSnapShots() 类型为 (z, etw) -> stateValue
      // 获得尚未完成的地方的 key distribution
      println(s"----- stateSnapshots $time -----")
      val rdd1 = rdd.partitionBy(new HashPartitioner(r)).mapPartitions(localMergeS)

      // input e: {(z, etw) -> local sum}
      // output {etw -> (M(total load), K, maxM, HeadSet)}, Dispersion
      val (newEtwInfo, dispersionS) = rdd1.aggregate(mutable.Map[BigInt, (Int, Int, Int, mutable.Set[String])](), 0)(
        (init, e) => {
          // 先获得 e 的信息看看是哪个 etw 的
          val ((z, etw), zSum) = e
          val (mm, k, maxM, headSet) = init._1.getOrElse(etw, (0, 0, 0, mutable.Set[String]()))
          // 对于 这个 etw 上的 这个 zsum 值判断是否是 heavyhitter
          if (zSum > ReduceMate.globalSumS.getOrElse(etw, 0) * 0.2 / m) {
            val info = init._1.getOrElse(etw, mutable.Set[String]())
            headSet.add(z)
          }
          val d = ReduceMate.dispersionS
          init._1(etw) = (mm + zSum, k + 1, Math.max(maxM, zSum), headSet)
          (init._1, d)
        }
        , (init, b) => b
      )

      // {etw -> (strategyId, HeavyhitterSet)}
      val etwOldInfo = (myBroadcastUnfinished.value ++ myBroadcastJustFinished.value).map(kv => (kv._1, (kv._2._1, kv._2._5)))

//      {etw -> (strategyId, M, K, p1, H)}
      val etwRet = newEtwInfo.map(kv => {
        val etw = kv._1
        val (mm, k, maxK, preheadSet) = kv._2
        val p1 = maxK * 1.0 / mm
        val (oldStrategy, oldSet) = etwOldInfo.getOrElse(etw, (0, Set[String]()))
        for (h <- oldSet) {
          // heavy hitter priority
          if (!preheadSet.contains(h)) {
            preheadSet.add(h)
          }
        }
  //    strategy priority
        val strategyId = Math.max(getStrategy(mm, k, m, p1, lambda, preheadSet.size), oldStrategy)

        (etw, (strategyId, mm, k, p1, preheadSet.toSet))
      }).toMap

      for (eh <- etwRet) {
        println(s"state: etw -> ${eh._1}, predict StrategyId -> ${eh._2._1}, " +
          s"M: ${eh._2._2}, K: ${eh._2._3}, p1: ${eh._2._4}, heavy hitters: ${eh._2._5.mkString(",")}")
      }
      println(s"DispersionS: $dispersionS")
      myBroadcastUnfinished.update(etwRet, true)
      println()
    })

    messages.foreachRDD((rdd, time) => {
      println(s"----- emitContent $time -----")
      val rdd1 = rdd.filter(!_.equals(None))
        .map(_.get)
        .partitionBy(new HashPartitioner(r))
        .mapPartitions(localMergeE)
      // (z, etw) -> stateValue

      // input e: {(z, etw) -> local sum}
      // output {etw -> (M(total load), K, maxM, HeadSet)}, Dispersion
      val (newEtwInfo, dispersionE) = rdd1.aggregate(mutable.Map[BigInt, (Int, Int, Int, mutable.Set[String])](), 0)(
        (init, e) => {
          // 先获得 e 的信息看看是哪个 etw 的
          val ((z, etw), zSum) = e
          val (mm, k, maxM, headSet) = init._1.getOrElse(etw, (0, 0, 0, mutable.Set[String]()))
          // 对于 这个 etw 上的 这个 zsum 值判断是否是 heavyhitter
          if (zSum > ReduceMate.globalSumE.getOrElse(etw, 0) * 0.2 / m) {
            val info = init._1.getOrElse(etw, mutable.Set[String]())
            headSet.add(z)
          }
          val d = ReduceMate.dispersionE
          init._1(etw) = (mm + zSum, k + 1, Math.max(maxM, zSum), headSet)
          (init._1, d)
        }
        , (init, b) => b
      )

      val etwOldInfo = (myBroadcastUnfinished.value ++ myBroadcastJustFinished.value).map(kv => (kv._1, (kv._2._1, kv._2._5)))

      //      {etw -> (strategyId, M, K, p1, H)}
      val etwRet = newEtwInfo.map(kv => {
        val etw = kv._1
        val (mm, k, maxK, preheadSet) = kv._2
        val p1 = maxK * 1.0 / mm
        val (oldStrategy, oldSet) = etwOldInfo.getOrElse(etw, (0, Set[String]()))
        for (h <- oldSet) {
          // heavy hitter priority
          if (!preheadSet.contains(h)) {
            preheadSet.add(h)
          }
        }
        //    strategy priority
        val strategyId = Math.max(getStrategy(mm, k, m, p1, lambda, preheadSet.size), oldStrategy)
        (etw, (strategyId, mm, k, p1, preheadSet.toSet))
      }).toMap

      for (eh <- etwRet) {
        println(s"emit: etw -> ${eh._1}, predict StrategyId -> ${eh._2._1}, " +
          s"M: ${eh._2._2}, K: ${eh._2._3}, p1: ${eh._2._4}, heavy hitters: ${eh._2._5.mkString(",")}")
      }
      println(s"DispersionE: $dispersionE")
      myBroadcastUnfinished.update(etwRet, true)
      println()
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
