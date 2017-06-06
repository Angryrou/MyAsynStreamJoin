package cluster

/**
  * Created by kawhi on 10/05/2017.
  *
  * Demo
  * kafka => L loaders =(hash) => R reducers
  * L = 3, M = 10, R = 2
  * L is set in Kafka-Topic; M and R are set in this application(stream.json)
  *
  * Query
  * find the minimum count of each word in each port during each mini-batch
  */
import kafka.serializer.StringDecoder
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.collection.mutable
import myutils.MyUtils



object HashJoin {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: HashJoin_stateless <stream.json>")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))

    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("HashJoin_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
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

    // (word, port) => (word, (port, global_count)) => (word, min_count)
    val mergeAndCompute = (iter : Iterator[(String, Int)]) => {
      val ret = mutable.Map[String, mutable.Map[Int, Int]]()
      while (iter.hasNext) {
        val wp = iter.next() // (word, port)
        val tmpMap = ret.getOrElse(wp._1, mutable.Map[Int, Int]())
        MyUtils.sleepNanos(sleep_time_map_ns)
//        Thread.sleep(1)
        tmpMap(wp._2) = tmpMap.getOrElse(wp._2, 0) + 1
        ret(wp._1) = tmpMap
      }
      val res = ret.map(t => (t._1, t._2.values.min))
      res.iterator
    }

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .flatMap(_._2.split(";"))
      .map(preProcessing) // (word, port)
      .transform(_.partitionBy(new HashPartitioner(m)))
      .mapPartitions(mergeAndCompute)

    messages.foreachRDD(_.collect())
    ssc.start()
    ssc.awaitTermination()
  }
}
