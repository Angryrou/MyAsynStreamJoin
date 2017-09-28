package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.HHPartitioner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kawhi on 23/09/2017.
  */
object HHJoin {

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: HashJoin_stateless <stream.json> duplicateRate")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val duplicateRate = Integer.parseInt(args(1))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("HHJoin_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // input type: (porti, "ts z x;*;*;*")
    // output type: (z, (porti, x))
    val preProcess = (iter : Iterator[(String, String)]) => {
      val ret = mutable.ListBuffer[(String, (Int, Int))]() // return type (z, (portId, x))
      while (iter.hasNext) {
        val tmp = iter.next() // (portId, "ts z x;ts z x;...;ts z x")
        val portId = tmp._1.toInt
        val strs = tmp._2.split(";")
        for (str <- strs) {
          val strTmp = str.split(' ')
          val z = strTmp(1)
          val x = strTmp(2).toInt
          for (i <- 1 to duplicateRate) {
            ret += (z ->(portId, x))
          }
        }
      }
      ret.iterator
    }

    val localJoin = (iter: Iterator[(String, (Int, Int))]) => {
      val globalMap = mutable.Map[String, mutable.Map[Int, BigInt]]()
      while (iter.hasNext){
        val tmp = iter.next() // (z, (portId, x))
        val keyMap = globalMap.getOrElse(tmp._1, mutable.Map[Int, BigInt]())
        keyMap(tmp._2._1) = keyMap.getOrElse(tmp._2._1, BigInt(0)) + BigInt(1)
        // map delay
        MyUtils.sleepNanos(sleep_time_map_ns)
        globalMap(tmp._1) = keyMap
      }
      val globalIter = globalMap.toIterator
      val ret = ArrayBuffer[(String, BigInt)]()
      while (globalIter.hasNext) {
        val tmp = globalIter.next()
        val keyMap = tmp._2
        val n = keyMap.getOrElse(0, BigInt(0)) * keyMap.getOrElse(1, BigInt(0)) * keyMap.getOrElse(2, BigInt(0))
        ret.append((tmp._1, n))
      }
      ret.iterator
    }

    // (porti, "ts z x;*;*;*")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .mapPartitions(preProcess) // (z, (portId, x))
      .transform(_.partitionBy(new HHPartitioner(m, 1)))
      .mapPartitions(localJoin)

    messages.foreachRDD((rdd, time) => {
      rdd.foreach(println)
      println(s"----- $time -----")
      println()
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
