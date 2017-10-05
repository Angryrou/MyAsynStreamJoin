package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.APKPartitioner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kawhi on 08/08/2017.
  */
object APKGrouping {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: APKGrouping_stateless <stream.json> 1,2 duplicateRate")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val duplicateRate = Integer.parseInt(args(2))
    val seeds= args(1).split(",").map(_.toInt)

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("APKGrouping_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // Broadcast
    val myBroadcast = BroadcastWrapper[Set[String]](ssc, Set[String]())
//    val myBroadcast = BroadcastWrapper[String](ssc, "")
    //    val strategy = BroadcastWrapper[Int](ssc, 0)

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
        MyUtils.sleepNanos(sleep_time_map_ns)
      }
      ret.iterator
    }

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: (z, 1)
    val pre = (id: Int, iter : Iterator[(String, String)]) => {
      val head = myBroadcast.value
      // 将新head存入;每个executor可能会有多个partition,所以要按照 partition id 存储
      println(s"loader-$id, head:${head.mkString(",")}")
      APKMate.updateHeadTable(id, head)
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

    // r = 1 的 tricky, 如果 r > 1 的话,分别给出每个 Reducer 上的 top-k. 然后给结果.
    val reduceLocalCompute = (iter : Iterator[(String, Int)]) => {
      val ret = mutable.Map[String, Int]()
      var sum = 0
      while (iter.hasNext) {
        val w = iter.next() // (word, local_count)
        ret.get(w._1) match {
          case Some(v) => {
            ret(w._1) = v + w._2
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
      heavyRet.iterator
    }

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
//      .flatMap(_._2.split(";"))
      .transform(_.mapPartitionsWithIndex(pre))
      .transform(_.partitionBy(new APKPartitioner(m, seeds)))
      .mapPartitions(mapLocalCompute)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(reduceLocalCompute)

    messages.foreachRDD((rdd, time) => {
      println(s"------ $time ------")
      val newHead = rdd.aggregate(mutable.Set[String]()) (
        (init, e) => {
          println(e)
          if (e._2._2 == true)
            init.add(e._1)
          init
        },
        (init, b)=> b
      ).toSet
      println(s"newHead: ${newHead.mkString(",")}")
      myBroadcast.update(newHead, true)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
