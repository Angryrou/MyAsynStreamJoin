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
    if (args.length != 3) {
      System.err.println("Usage: DGrouping_stateless <stream.json> duplicateRate lambda")
      System.exit(1)
    }

    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val duplicateRate = Integer.parseInt(args(1))
    val lambda = args(2).toDouble

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("DGrouping_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // Broadcast
    val myBroadcast = BroadcastWrapper[(Double, Int)](ssc, (0.0, 600))
    //    val strategy = BroadcastWrapper[Int](ssc, 0)


    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: (z, 1)
    val preProcess = (iter: Iterator[String]) => {
      DMate.p1 = myBroadcast.value._1
      DMate.k = myBroadcast.value._2
      DMate.lambda = lambda
      DMate.m = m
      //      strategy.update(DMate.getStrategy(), true)
      val ret = mutable.ListBuffer[(String, Int)]() // return type
      while (iter.hasNext) {
        val tmp = iter.next().split(' ')
        val z = tmp(1)
        val x = tmp(2).toInt
        for (a <- 1 to duplicateRate) {
          ret += (z -> 1)
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
      while (iter.hasNext) {
        val w = iter.next() // (word, local_count)
        ret.get(w._1) match {
          case Some(v) => {
            ret(w._1) = v + w._2
            MyUtils.sleepNanos(sleep_time_reduce_ns)
          }
          case None => ret(w._1) = w._2
        }
      }

      ret.iterator
    }

    // (porti, "ts z x;*;*;*")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";")) // "ts z x"
      .transform((rdd, time) => {
      rdd.mapPartitions((iter: Iterator[String]) => {
        DMate.p1 = myBroadcast.value._1
        DMate.k = myBroadcast.value._2
        DMate.lambda = lambda
        DMate.m = m
        println(s" $time --> ${DMate.p1}, ${DMate.k}")

        //      strategy.update(DMate.getStrategy(), true)
        val ret = mutable.ListBuffer[(String, Int)]() // return type
        while (iter.hasNext) {
          val tmp = iter.next().split(' ')
          val z = tmp(1)
          val x = tmp(2).toInt
          for (a <- 1 to duplicateRate) {
            ret += (z -> 1)
          }
        }
        ret.iterator
      })
    })
      //      .mapPartitions(preProcess) // "z x"
      .transform(_.partitionBy(new DPartitioner(m)))
      .mapPartitions(mapLocalCompute)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(reduceLocalCompute)

    messages.foreachRDD((rdd, time) => {
      rdd.foreach(println)
      val k = rdd.count().toInt
      val p1 = if (k == 0) 0.0 else rdd.map(_._2).max() * 1.0 / rdd.map(_._2).sum()
      myBroadcast.update((p1, k), true)
      println(s" $time ==> $p1, $k")
      println()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
