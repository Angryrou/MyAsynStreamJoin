package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.HHPartitioner

import scala.collection.mutable

/**
  * Created by kawhi on 08/08/2017.
  */
object HHGrouping {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: HHGrouping_stateless <stream.json> hash_seed")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val seed = Integer.parseInt(args(1))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("HHGrouping_stateless")
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
//        MyUtils.sleepNanos(sleep_time_map_ns)
      }
      ret.iterator
    }

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .map(w => (w, 1)) // (word, 1)
      .transform(_.partitionBy(new HHPartitioner(m, seed)))
      .mapPartitions(mapLocalCompute)

    messages.foreachRDD(_.collect())
    ssc.start()
    ssc.awaitTermination()
  }
}
