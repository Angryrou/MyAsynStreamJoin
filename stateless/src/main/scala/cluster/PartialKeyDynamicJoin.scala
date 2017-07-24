package cluster

import kafka.serializer.StringDecoder
import mypartitioner.{DynamicPartialKeyPartitioner, PartialKeyPartitioner}
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kawhi on 03/07/2017.
  */
object PartialKeyDynamicJoin {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: PartialKeyDynamicJoin <stream.json>")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("PartialKeyDynamicJoin_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // "timestamp port word" => (word, port) e.g. (A, 9999)
    val preProcessing = (str: String) => {
      val tmp = str.split(" ")
      (tmp(2), tmp(1).toInt)
    }

    // "timestamp port word" => (word, port) e.g. (A, 9999), find the optimum d.
    val pre = (id: Int, iter : Iterator[String]) => {
      val ret = ArrayBuffer[(String, Int)]()
      val wc = mutable.Map[String, Int]()
      var len = 0
      while (iter.hasNext) {
        val wp = iter.next().split(" ")
        wc(wp(2)) = wc.getOrElse(wp(2), 0) + 1
        len += 1
        ret.append((wp(2), wp(1).toInt))
      }
//      val p1 = if (len == 0) 0.0 else {
      val p1 = if (wc.size == 0) 0.0 else {
        wc.values.max * 1.0 / len
      }
      val d = (p1 * m).toInt + 1
      PartialKeyDynamicKey.updateDTables(id, d)
      println(s"loader-$id, p1 = $p1, d = $d, wc.size = ${wc.size}, len = $len")
      ret.iterator
    }

    // (word, port) => (word, (port, local_count))
    val localMerge = (iter : Iterator[(String, Int)]) => {
      val ret = mutable.Map[(String, Int), Int]()
      while (iter.hasNext) {
        val wp = iter.next() // (word, port)
        MyUtils.sleepNanos(sleep_time_map_ns)
        //        Thread.sleep(1)
        ret(wp) = ret.getOrElse(wp, 0) + 1 // ((word, port), local_count)
      }
      ret.toList.map(t => (t._1._1, (t._1._2, t._2))).iterator
    }

    // (word, (port, local_count)) => (word, (port, global_count)) => (word, min_count)
    val globalMerge = (iter : Iterator[(String, (Int, Int))]) => {
      val ret = mutable.Map[String, mutable.Map[Int, Int]]() // word, (port, global_count)
      while(iter.hasNext) {
        val wpc = iter.next() // (word, (port, lc))
        val tmpMap = ret.getOrElse(wpc._1, mutable.Map[Int, Int]())
        MyUtils.sleepNanos(sleep_time_reduce_ns)
        //        Thread.sleep(1)
        tmpMap(wpc._2._1) = tmpMap.getOrElse(wpc._2._1, 0) + wpc._2._2
        ret(wpc._1) = tmpMap
      }
      val res = ret.map(t => (t._1, t._2.values.min))
      res.iterator
    }

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .transform(_.mapPartitionsWithIndex(pre))
      .transform(_.partitionBy(new DynamicPartialKeyPartitioner(m)))
      .mapPartitions(localMerge) // (word, (port, local_count))
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(globalMerge)

    messages.foreachRDD(_.collect())
    ssc.start()
    ssc.awaitTermination()
  }
}
