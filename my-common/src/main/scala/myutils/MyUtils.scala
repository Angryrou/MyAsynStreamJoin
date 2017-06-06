package myutils

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/**
  * Created by kawhi on 03/06/2017.
  */

object MyUtils {

  def getFromJson(input: String) = {
    val source: String = Source.fromFile(input).getLines.mkString
    if (source == null) {
      System.err.println(
        s"""
           | stream.json is not found or empty, please check!
         """.stripMargin
      )
      System.exit(1)
    }
    val jMap = parse(source)
    val brokers = (jMap \ "brokers").values.toString
    val topics = (jMap \ "topics").values.toString.split(",").toSet
    val batch_duration = (jMap \ "batch_duration").values.toString.toInt
    val ports_str = (jMap \ "ports").values.toString
    val M = (jMap \ "#mapper").values.toString.toInt
    val R = (jMap \ "#reducer").values.toString.toInt
    val kafka_offset = (jMap \ "kafka_offset").values.toString
    val path = (jMap \ "save_path").values.toString
    val lgw = (jMap \ "logic_time_slot").values.toString.toInt
    val sleep_time_ns = ((jMap \ "sleep_time_ms").asInstanceOf[JDouble].values * 1000000).toLong
    val sleep_time_map_ns = ((jMap \ "sleep_time_map_ms").asInstanceOf[JDouble].values * 1000000).toLong
    val sleep_time_reduce_ns = ((jMap \ "sleep_time_reduce_ms").asInstanceOf[JDouble].values * 1000000).toLong

    //    val key_space = (jMap \ "key_space").values.toString
    val key_space = (jMap \ "key_space").values.asInstanceOf[List[String]]
    val parsePorts = (ps_str: String) => {
      if (ps_str.split(",").length != 3) {
        println("ports syntx ERROR!")
        System.exit(1)
      }
      val Array(bg, ct, st) = ps_str.split(",").map(_.toInt)
      val ps = new Array[Int](ct)
      for (i <- 0 until ct) {
        ps(i) = bg + st * i
      }
      ps
    }
    val ports = parsePorts(ports_str)
    val ports_num = ports.length
    (brokers, topics, batch_duration, ports_num, M, R, kafka_offset, path,
      lgw, key_space, sleep_time_map_ns, sleep_time_reduce_ns)
  }
  def sleepNanos(internal : Long = 100) = {
    val start = System.nanoTime()
    var end : Long = 0
    do{
      end = System.nanoTime()
    }while(start + internal >= end)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}
