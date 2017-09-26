package cluster

import kafka.serializer.StringDecoder
import myutils.MyUtils
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import partitioner.{DuplicateHHPartitioner, HHPartitioner}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.google.common.hash.Hashing

/**
  * Created by kawhi on 25/09/2017.
  */
object DuplicateHHJoin {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: DuplicateHashJoin_stateless <stream.json> duplicateRate")
      System.exit(1)
    }
    // 参数读取
    val (brokers, topics, batch_duration, ports_num, m, r, kafka_offset, path, lgw, key_space, sleep_time_map_ns,
    sleep_time_reduce_ns) = MyUtils.getFromJson(args(0))
    val duplicateRate = Integer.parseInt(args(1))

    val l = 3
    // new 一个 streamingContext
    val sc = new SparkConf().setAppName("DuplicateHHJoin_stateless")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(batch_duration))

    // -> Broadcast (M, Map[h -> (pId_start, pId_length, (mx, my, mz))])
    // x Broadcast (M, Map[h -> (pId_start, pId_length, \sum, \prod)])
    val myBroadcast = BroadcastWrapper[(Int, Map[String, (Int, Int, (Int, Int, Int))])](ssc, (450000, HashMap()))

    // kafka 配置接入
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> kafka_offset
    )

    // input: (porti, "ts z x;*;*")
    // output: ((-1/0~m-1, z), (port, x)):
    // ---- Heavy hitter: ((0~m-1, z), (port, x));
    // ---- Light key: ((-1, z), (port, x))
    val preProcess = (iter: Iterator[(String, String)]) => {
      val ret = mutable.ListBuffer[((Int, String), (Int, Int))]() // return type ((-1~m-1, z), (portId, x))
      val heavy = myBroadcast.value._2
      val seed = 11
      val portHashes = Array(0,1,2).map(p => {
        Hashing.murmur3_128(seed + p)
      }) // heavy -> hash

      while (iter.hasNext) {
        val tmp = iter.next() // (portId, "ts z x;ts z x;...;ts z x")
        val portId = tmp._1.toInt
        val strs = tmp._2.split(";")
        for (str <- strs) {
          val strTmp = str.split(' ')
          val z = strTmp(1)
          val x = strTmp(2)
          if (heavy.contains(z)) {
            // z 将要被分到的地方
            // 来自 port 1 的 z: (hash1(x1), *, *)
            // 来自 port 2 的 z: (*, hash2(x2), *)
            // 来自 port 3 的 z: (*, *, hash3(x3))
            // pId_start, pId_length, (mx, my, mz)
            val (pId_start, pId_length, (mx, my, mz)) = heavy.get(z).get
            val mArr = Array(mx, my, mz)
            val offsetList = mutable.ListBuffer[Int]()
            val p = (portHashes(portId).hashBytes(x.getBytes()).asLong() % pId_length).toInt
            for(i <- 0 until pId_length) {
              if (i % mArr(portId) == p)
                offsetList.append(i)
            }
            if (offsetList.isEmpty) {
              for (i <- 1 to duplicateRate) {
                ret += ((-1, z) ->(portId, x.toInt))
              }
            } else {
              for (o <- offsetList) {
                for (i <- 1 to duplicateRate) {
                  ret += (o, z) ->(portId, x.toInt)
                }
              }
            }
          } else {
            for (i <- 1 to duplicateRate) {
              ret += ((-1, z) ->(portId, x.toInt))
            }
          }
        }
      }
      ret.iterator
    }

    // input: ((*, z), (portId, x))
    // x output: (z, (M1(k), M2(k), M3(k))), 在reduce 那边要多遍历一遍
    // -> output: (z, (join_result, (M1(k), M2(k), M3(k))))
    // x output: (z, (join_result(/prod), /sum), 对于 heavy hitter load 需要落实到数据从哪个 loader 来
    val localJoin = (iter: Iterator[((Int, String), (Int, Int))]) => {
      // globalMap: z -> {porti -> key个数}
      val globalMap = mutable.Map[String, mutable.Map[Int, Int]]()
      while (iter.hasNext) {
        val tmp = iter.next() // ((*, z), (portId, x))
        val keyMap = globalMap.getOrElse(tmp._1._2, mutable.Map[Int, Int]())
        keyMap(tmp._2._1) = keyMap.getOrElse(tmp._2._1, 0) + 1
        globalMap(tmp._1._2) = keyMap
      }
      val globalIter = globalMap.toIterator
      val ret = ArrayBuffer[(String, (Int, (Int, Int, Int)))]() // return type:(z, (join_result, (M1(k), M2(k), M3(k))))
      while (globalIter.hasNext) {
        val tmp = globalIter.next()
        val keyMap = tmp._2
        val m1 = keyMap.getOrElse(0, 0)
        val m2 = keyMap.getOrElse(1, 0)
        val m3 = keyMap.getOrElse(2, 0)
        ret.append((tmp._1, (m1 * m2 * m3, (m1, m2, m3))))
      }
      ret.iterator
    }

    // input: (z, (\prod, (M1(z), M2(z), M3(z)))) M1 M2 M3 含有冗余
    // output: (z, (star-join-group-by result, /sum, (M1, M2, M3))) 去除冗余的 M1 M2 M3
    val reduceLocalCompute = (iter: Iterator[(String, (Int, (Int, Int, Int)))]) => {
      val oldHeavy = myBroadcast.value._2
      val heavySet = oldHeavy.keySet

      // (z, (joinRes, (M1, M2, M3)) 含冗余
      val mergeRes = mutable.Map[String, (Int, (Int, Int, Int))]()
      while (iter.hasNext) {
        val e = iter.next()
        if (heavySet.contains(e._1)) { // heavy hitter
          val partialJoin = e._2._1
          val partialM = e._2._2
          mergeRes.get(e._1) match {
            case Some(tmp) => mergeRes(e._1) = (partialJoin + tmp._1,
                (partialM._1 + tmp._2._1, partialM._2 + tmp._2._2, partialM._3 + tmp._2._3))
            case None => mergeRes(e._1) = e._2
          }
        } else {
          mergeRes(e._1) = e._2
        }
      }

      // (z, (joinRes(\prod), \sum, (m1, m2, m3))) 去冗余的sum
      var sum = 0
      val res = mergeRes.map(zr => {
        oldHeavy.get(zr._1) match {
          case Some(e) => {
            val (mx, my, mz) = e._3
            val r = zr._2
            val m1 = r._2._1 / my / mz
            val m2 = r._2._2 / mx / mz
            val m3 = r._2._3 / mx / my
            val Mz = m1 + m2 + m3
            sum += Mz
            (zr._1, (r._1, m1+m2+m3, (m1, m2, m3)))
          }
          case None => {
            val r = zr._2
            val Mz = r._2._1 + r._2._2 + r._2._3
            sum += Mz
            (zr._1, (r._1, Mz, (r._2._1, r._2._2, r._2._3)))
          }
        }
      })
      ReduceMate.globalSum = sum
      res.iterator
    }

    // (porti, "ts z x;*;*;*")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
      .mapPartitions(preProcess) // (z, (portId, x))
      .transform(_.partitionBy(new DuplicateHHPartitioner(m, 1)))
      .mapPartitions(localJoin)
      .transform(_.partitionBy(new HashPartitioner(r)))
      .mapPartitions(reduceLocalCompute)

    messages.foreachRDD((rdd, time) => {
      println(s"------ $time ------")
      // (K, HeavyMap(key -> \prod, \sum, (M1, M2, M3), M, tmp1) tmp1: \sum ((\prod)^0.5)
      val info = rdd.aggregate((0, mutable.HashMap[String, (Int, Int, (Int, Int, Int))](), 0, 0.0)) (
        (partial, e) => {
          // e : (z, (/prod, /sum, (M1, M2, M3)))
          println(e)
          val K = partial._1 + 1
          val inc = partial._3 + e._2._2
          var tmp1 = partial._4
          if (e._2._2 > ReduceMate.globalSum * 1.0 / m) {
            // heavy hitter
            partial._2(e._1) = e._2
            println(s"发现一个 heavy hitter: ${e._1}, prod: ${e._2._1}")
            tmp1 += math.pow((e._2._1), 0.5)
          }
          (K, partial._2, inc, tmp1)
        },
        (a, b) => b
      )
      val K = info._1
      val heavyMap = info._2
      val M = info._3
      val tmp1 = info._4
      println(s"M = $M, K 的个数: $K, heavy hitter 个数: ${heavyMap.size}")
      // 计算每个 heavy hitter 分别取几台机器
      println(s"tmp1: $tmp1")
      var pId_start = 0
      var pId_length = 0
      val heavyWorkers = heavyMap.map(hv => {
        pId_start += pId_length
        val h = hv._1
        val v = hv._2
        val mh = (math.pow(v._1, 0.5) / tmp1 * m).toInt // heavy hitter h 分配到的 worker 的个数
        println(s"heavy hitter: $h, 连乘: ${v._1}")
        if (mh < 1) {
          println(s"报警了,分配个数小于1, heavy hitter: $h, p = ${v._2 * 1.0 / M}")
        }
        println(s"hitter: $h, worker 个数分配:$mh")
        val tmp2 = 3 * math.pow(v._1/mh/mh, 0.333)
        var mx = math.ceil(mh * tmp2 / v._3._1).toInt
        var my = math.ceil(mh * tmp2 / v._3._2).toInt
        var mz = math.ceil(mh * tmp2 / v._3._3).toInt
        while (mx * my * mz > mh) {
          println(s"发现异常,调整一次: mx: $mx, my: $my, mz: $mz")
          if (mx >= my && mx >= mz) {
            mx -= 1
          } else if (my >= mz) {
            my -= 1
          } else {
            mz -= 1
          }
        }
        if (mx == 0 || my == 0 || mz == 0) {
          println(s"mx = $mx, my = $my, mz = $mz. 没法玩了, hash 底数是 0")
        }
        pId_length = mh
        (h, (pId_start, mx*my*mz, (mx, my, mz)))
      })

      myBroadcast.update((ReduceMate.globalSum, heavyWorkers.toMap), true)

      println()
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
