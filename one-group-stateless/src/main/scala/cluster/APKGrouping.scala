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
    if (args.length != 1) {
      System.err.println("Usage: APKGrouping_stateless <stream.json>")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("APKGrouping_stateless")
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

    // "word" => (word, 1) e.g. (A, 10), find the head
    val pre = (id: Int, iter : Iterator[String]) => {
      val ret = ArrayBuffer[(String, Int)]()
      val wc = mutable.Map[String, Int]()
      var len = 0
      while (iter.hasNext) {
        val w = iter.next()
        wc(w) = wc.getOrElse(w, 0) + 1
        len += 1
        ret.append((w, 1))
      }
      val head = mutable.Set[String]()
      val threshold = 0.2 / m
      wc.foreach(kv => {
        if (kv._2 * 1.0 / len > threshold) {
          head.add(kv._1)
        }
      })
      APKConfig.updateHeadTable(id, head.toSet)
      // 将新head存入;每个executor可能会有多个partition,所以要按照 partition id 存储
      println(s"loader-$id, head:${head.mkString(",")}, wc.size = ${wc.size}, len = $len")
      ret.iterator
    }

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
//      .map(w => (w, 1)) // (word, 1)
        .transform(_.mapPartitionsWithIndex(pre))
      .transform(_.partitionBy(new APKPartitioner(m)))
      .mapPartitions(mapLocalCompute)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(reduceLocalCompute)

    messages.foreachRDD(_.collect())
    ssc.start()
    ssc.awaitTermination()
  }
}
