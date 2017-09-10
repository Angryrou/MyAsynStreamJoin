package timetable

import scala.collection.concurrent.TrieMap

/**
  * Created by kawhi on 03/06/2017.
  */
object MyStateJoinUtils {

  // partitionId -> { relationId -> ltw }
  // Every partition has its own timeTable who keeps the arrived logical-time-window time(aka. ltw)
  private val timeTables = TrieMap[Int, TrieMap[Int, BigInt]]()

  // Every partition has its own global_trigger
  private val partitionTriggers = TrieMap[Int, BigInt]()

  def getPartitionTriggers(partitionId : Int) : BigInt = {
    partitionTriggers.getOrElse(partitionId, 0)
  }

  def setPartitionTriggers(partitionId : Int, trigger : BigInt): Unit = {
    partitionTriggers(partitionId) = trigger
  }

  def updataTimeTablesAndGetTriggers(partitionId: Int, relationId: Int, relation_num: Int, relation_trigger: BigInt): Option[BigInt] = {
    // 在 partitionId 上接收到某 port 上 port_trigger 时间窗口及以前的数据已经全部收集完成.
    // 0. 取出属于当前 partitionId 的时间表 mp
    val mp = timeTables.getOrElse(partitionId, TrieMap[Int, BigInt]())
    if (mp.size == relation_num) {
      val old_global_trigger = getPartitionTriggers(partitionId)
      mp(relationId) = relation_trigger
      // 更新timeTables
      timeTables(partitionId) = mp
      val new_global_trigger = mp.values.min
      // 如果各个 port 上已经到达的时间最小值更新了,说明产生了新的 trigger

      if (new_global_trigger > old_global_trigger) {
        //将 global_trigger 更新存入
        setPartitionTriggers(partitionId, new_global_trigger)
        return Some(new_global_trigger)
      } else if (new_global_trigger == old_global_trigger){
        return None
      } else {
        Console.err.println(s"out-of-order: old_trigger: $old_global_trigger, new_trigger: $new_global_trigger ")
        return None
      }
    } else {
      //      println(s"partition: $partitionId, port: $port, port_num: $port_num, port_trigger: $port_trigger\n" +
      //        s"input mp: $mp\n" +
      //        s"mp.size < port_num\n")
      mp(relationId) = relation_trigger
      // 更新 timeTables
      timeTables(partitionId) = mp

      // 若刚刚收集器各个 port 上的已到达的时间最小值,说明产生了第一个 trigger
      if (mp.size == relation_num) {
        val global_trigger = mp.values.min
        // 将 global_trigger 更新存入
        setPartitionTriggers(partitionId, global_trigger)
        return Some(global_trigger)
      } else {
        return None
      }
    }
  }
}
