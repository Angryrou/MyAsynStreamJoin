package cluster

import kafka.serializer.StringDecoder
import mypartitioner.{AdvancedPartialKeyPartitioner, PartialKeyPartitioner}
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kawhi on 24/07/2017.
  * Demo
  * kafka => L loaders =(partial-key)=> M mappers =(hashPartition)=> R reducers
  * L = 3, M = 10, R = 2
  * L is set in Kafka-Topic; M and R are set in this application(stream.json)
  *
  * Query
  * find the minimum count of each word in each port during each mini-batch
  */

object AdvancedPartialKeyJoin {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: AdvancedPartialKeyJoin <stream.json> 13,19")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val seeds= args(1).split(",").map(_.toInt)
    if (seeds.size < 2) {
      System.err.println("head choices cannot less than 2")
    }
    val threshold = 0.2 / m
    val head = mutable.Set[String]()

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("AdvancedPartialKeyJoin_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

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
      head.clear()
      wc.foreach(kv => {
        if (kv._2 * 1.0 / len > threshold) {
          head.add(kv._1)
        }
      })
      AdvancedConfig.updateHeadTable(id, head.toSet)
      println(s"loader-$id, head:${head.mkString(",")}, wc.size = ${wc.size}, len = $len")
      ret.iterator
    }

    // (word, port) => (word, (port, local_count))
    val localMerge = (iter : Iterator[(String, Int)]) => {
      val ret = mutable.Map[(String, Int), Int]()
      while (iter.hasNext) {
        val wp = iter.next() // (word, port)
        MyUtils.sleepNanos(sleep_time_map_ns)
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
      .transform(_.partitionBy(new AdvancedPartialKeyPartitioner(m, seeds)))
      .mapPartitions(localMerge) // (word, (port, local_count))
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(globalMerge)

    messages.foreachRDD(_.collect())
    ssc.start()
    ssc.awaitTermination()
  }
}
