package local.test

import com.google.common.hash.Hashing
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/**
  * Created by kawhi on 03/06/2017.
  */
object JsonLoadTest {
  val getFromJson = (input: String) => {
    val source: String = Source.fromFile("stream.json").getLines.mkString
    if (source == null) {
      System.err.println(
        s"""
           | stream.json is not found or empty, please check!
         """.stripMargin
      )
      System.exit(1)
    }
    val jMap = parse(source)
    val brokers = (jMap \ "brokers").values
    val topics = (jMap \ "topics").values.toString.split(",").toSet
    val batch_duration = (jMap \ "batch_duration").values.toString.toLong
    val ports_str = (jMap \ "ports").values.toString
    val M = (jMap \ "#mapper").values.toString.toInt
    val R = (jMap \ "#reducer").values.toString.toInt
    val kafka_offset = (jMap \ "kafka_offset").values.toString
    val path = (jMap \ "save_path").values
    val lgw = (jMap \ "logic_time_slot").values.toString.toInt
    val key_space = (jMap \ "key_space").values.asInstanceOf[List[String]]
    val sleep_time_ns = ((jMap \ "sleep_time_ms").asInstanceOf[JDouble].values * 1000000).toLong
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
    (brokers, topics, batch_duration, ports_num, M, R, kafka_offset, path, lgw, key_space, sleep_time_ns)
  }
  def main(args: Array[String]) {

    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_ns)
    = getFromJson("stream.json")
    println(brokers, brokers.getClass)
    println(topics, topics.getClass)
    println(batch_duration, batch_duration.getClass)
    println(ports_num, ports_num.getClass)
    println(m, m.getClass)
    println(r, r.getClass)
    println(kafka_offset, kafka_offset.getClass)
    println(path, path.getClass)
    println(lgw, lgw.getClass)
    println(key_space, key_space.getClass)
    println(sleep_time_ns, sleep_time_ns.getClass)

  }
}
