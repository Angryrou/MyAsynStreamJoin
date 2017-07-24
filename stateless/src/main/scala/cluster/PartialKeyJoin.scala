package cluster

/**
  * Created by kawhi on 10/05/2017.
  *
  * Demo
  * kafka => L loaders =(partial-key)=> M mappers =(hashPartition)=> R reducers
  * L = 3, M = 10, R = 2
  * L is set in Kafka-Topic; M and R are set in this application(stream.json)
  *
  * Query
  * find the minimum count of each word in each port during each mini-batch
  */
import mypartitioner.PartialKeyPartitioner
import myutils.MyUtils
import kafka.serializer.StringDecoder
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

object PartialKeyJoin {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: PartialKeyJoin <stream.json> 13,19")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val seeds= args(1).split(",").map(_.toInt)

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("PartialKeyJoin_stateless")
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
      .map(preProcessing) // (word, count)
      .transform(_.partitionBy(new PartialKeyPartitioner(m, seeds)))
      .mapPartitions(localMerge) // (word, (port, local_count))
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(globalMerge)

    messages.foreachRDD(_.collect())
    ssc.start()
    ssc.awaitTermination()
  }
}


