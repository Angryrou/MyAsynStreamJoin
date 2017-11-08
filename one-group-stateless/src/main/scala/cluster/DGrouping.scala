package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.DPartitioner

import scala.collection.mutable

/**
  * Created by kawhi on 08/08/2017.
  */
object DGrouping {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: DGrouping_stateless <stream.json> 13,19 duplicateRate lambda")
      System.exit(1)
    }

    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val seeds= args(1).split(",").map(_.toInt)
    val duplicateRate = Integer.parseInt(args(2))
    val lambda = args(3).toDouble

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("DGrouping_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // Broadcast (M, K, p1, H)
    val myBroadcast = BroadcastWrapper[(Int, Int, Double, Set[String], Int)](ssc, (150000, 3000, 0.0, Set[String](), 3000))
    //    val strategy = BroadcastWrapper[Int](ssc, 0)


    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: (z, 1)
    val preProcess = (id: Int, iter: Iterator[(String, String)]) => {
      DMate.p1 = myBroadcast.value._3
      DMate.K = myBroadcast.value._2
      DMate.M = myBroadcast.value._1
      DMate.single = myBroadcast.value._5
      DMate.lambda = lambda
      DMate.m = m
      // 将新head存入;每个executor可能会有多个partition,所以要按照 partition id 存储
      DMate.updateHeadTable(id, myBroadcast.value._4)

      val ret = mutable.ListBuffer[(String, Int)]() // return type
      while (iter.hasNext) {
        val tmp10 = iter.next()._2.split(";")
        for (t <- tmp10) {
          val tmp = t.split(' ')
          val z = tmp(1)
          //        val x = tmp(2).toInt
          for (a <- 1 to duplicateRate) {
            ret += (z -> 1)
          }
        }
      }
      ret.iterator
    }

    val mapLocalCompute = (iter: Iterator[(String, Int)]) => {
      val ret = mutable.Map[String, Int]()
      while (iter.hasNext) {
        val w = iter.next() // (word, 1)
        ret(w._1) = ret.getOrElse(w._1, 0) + 1
        MyUtils.sleepNanos(sleep_time_map_ns)
      }
      ret.iterator
    }

    val reduceLocalCompute = (iter: Iterator[(String, Int)]) => {
      val ret = mutable.Map[String, Int]()
      var sum = 0
      var dispersion = 0
      while (iter.hasNext) {
        val w = iter.next() // (word, local_count)
        ret.get(w._1) match {
          case Some(v) => {
            ret(w._1) = v + w._2
            dispersion += 1
            MyUtils.sleepNanos(sleep_time_reduce_ns)
          }
          case None => ret(w._1) = w._2
        }
        sum = sum + w._2
      }

      val heavyRet = ret.map(r => (r._1, (r._2, false)))

      for(wc <- heavyRet) {
        if (wc._2._1 > sum * 0.2 / m) {
          heavyRet(wc._1) = (wc._2._1, true)
        }
      }
      DMate.dispersion = dispersion
      heavyRet.iterator
    }

    // (porti, "ts z x;*;*;*")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
//      .flatMap(_._2.split(";")) // "ts z x"
      .transform(_.mapPartitionsWithIndex(preProcess))
      .transform(_.partitionBy(new DPartitioner(m, seeds)))
      .mapPartitions(mapLocalCompute)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(reduceLocalCompute)


    def getStrategy(M:Int, K:Int, m:Int, p1: Double, lambda: Double, headNum: Int, sin: Int):Int = {

      val costHH = (14.55 * p1 - 0.3857 + 1.0) * M / m

//      val costAPK = M/m + lambda * dis
      val costAPK = M/m + lambda * (K + headNum * (m - 2) - sin)


      // 0 for HH, 1 for APK
      val ret = if (costHH <= costAPK) 0 else 1
      println(s"p1: $p1,  costHH: $costHH, costAPK: $costAPK, strategy: $ret")
      ret
    }

    messages.foreachRDD((rdd, time) => {
      println(s"------ $time ------")
      // M(total load), K, maxM(k), HeadSet, Dispersion

      val info = rdd.aggregate((0, 0, 0, mutable.Set[String](), 0, 0)) (
        (init, e) => {
          println(e)
          val M = init._1 + e._2._1
          val K = init._2 + 1
          val D = DMate.dispersion
          val maxK = if(init._3 > e._2._1) init._3 else e._2._1
          if (e._2._2 == true)
            init._4.add(e._1)
          val single = if (e._2._1 <= 2) {
            init._6 + 1
          } else {
            init._6
          }

          (M, K, maxK, init._4, D, single)
        },
        (a,b) => b
      )

      val M = info._1
      val K = info._2
      val p1 = info._3 * 1.0 / M
      val newHead = info._4.toSet
      val D = info._5
      val single = info._6

      val partitionId = getStrategy(M, K, m, p1, lambda, newHead.size, single)
      println(s"partitionId: $partitionId")
      println(s"M: $M, K: $K, p1: $p1, dispersion: $D, newHead: ${newHead.mkString(",")}, single: $single, gap: ${K-single}")
      myBroadcast.update((M, K, p1, newHead, single), true)
      println()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
