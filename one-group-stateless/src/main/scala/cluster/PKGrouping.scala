package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.PKPartitioner

import scala.collection.mutable

/**
  * Created by kawhi on 08/08/2017.
  */
object PKGrouping {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: PKGrouping_stateless <stream.json> duplicateRate")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val seeds = Array(1,2)
    val duplicateRate = Integer.parseInt(args(1))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("PKGrouping_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

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

    val reduceLocalCompute = (iter : Iterator[(String, Int)]) => {
      val ret = mutable.Map[String, Int]()
      while (iter.hasNext) {
        val w = iter.next() // (word, local_count)
        ret(w._1) = ret.getOrElse(w._1, 0) + w._2
        MyUtils.sleepNanos(sleep_time_reduce_ns)
      }
      ret.iterator
    }

    // input: "timestamp AAA 999" (ts, z, x) 均来自同一个 relation,所以 timestamp 的数据有序
    // output: (z, x)
    val preProcess = (iter : Iterator[String]) => {
      val ret = mutable.ListBuffer[(String, Int)]() // return type
      while (iter.hasNext) {
        val tmp = iter.next().split(' ')
        val z = tmp(1)
        val x = tmp(2).toInt
        for (a <- 1 to duplicateRate) {
          ret += (z -> x)
        }
      }
      ret.iterator
    }

    // (porti, "ts z x;*;*;*")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";")) // "ts z x"
      .mapPartitions(preProcess)
      .transform(_.partitionBy(new PKPartitioner(m, seeds)))
      .mapPartitions(mapLocalCompute)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(reduceLocalCompute)

    messages.foreachRDD((rdd, time) => {
      rdd.foreach(println)
      println(s"----- $time -----")
      println()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
