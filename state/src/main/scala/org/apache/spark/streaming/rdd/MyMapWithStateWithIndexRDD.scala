package org.apache.spark.streaming.rdd

/**
  * Created by kawhi on 2017/5/30.
  */

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.util.{EmptyStateMap, StateMap}
import org.apache.spark.streaming.{State, StateImpl, Time}
import org.apache.spark.util.Utils
import timetable.MyStateJoinUtils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Record storing the keyed-state [[MyMapWithStateWithIndexRDD]]. Each record contains a [[StateMap]] and a
  * sequence of records returned by the mapping function of `mapWithState`.
  */
private[streaming] case class MyMapWithStateWithIndexRDDRecord[K, S, E](
                                                                         var stateMap: StateMap[K, S], var mappedData: Seq[E])

private[streaming] object MyMapWithStateWithIndexRDDRecord {
  def updateRecordWithData[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
                                                                                m: Int,
                                                                                index: Int,
                                                                                prevRecord: Option[MyMapWithStateWithIndexRDDRecord[K, S, E]],
                                                                                dataIterator: Iterator[(K, V)],
                                                                                mappingFunction: (Int, Time, K, Option[V], State[S]) => Option[E],
                                                                                batchTime: Time,
                                                                                timeoutThresholdTime: Option[Long],
                                                                                removeTimedoutData: Boolean
                                                                              ): MyMapWithStateWithIndexRDDRecord[K, S, E] = {
    // Create a new state map by cloning the previous one (if it exists) or by creating an empty one
    val newStateMap = prevRecord.map {
      _.stateMap.copy()
    }.getOrElse {
      new EmptyStateMap[K, S]()
    }

    val mappedData = new ArrayBuffer[E]
    val wrappedState = new StateImpl[S]()

//    println(s"here is updateRecordWithData --> paritition $index, ports_num = $m")

    // Call the mapping function on each record in the data iterator, and accordingly
    // update the states touched, and collect the data returned by the mapping function
    // if the msg is a trigger signal, output the records

    // m: ports_num
    // input1: (word, (port, ltw))
    // input2: (mapWorkerPartitionId, (port, ltw))
    // 判断 input 标准: 传入的 key 是不是纯数字.

    val regex ="""^\d+$""".r
    dataIterator.foreach { case (key, value) =>
      val k = key.asInstanceOf[String]
      val v = value.asInstanceOf[(Int, BigInt)]
//      println(s"partition: $index, dataIterator: ($k , $v)")
      (regex.findFirstMatchIn(k) == None) match {
        case false => {
          // 说明 k 是纯数字的 字符串, 属于 input2
          val trigger = MyStateJoinUtils.updataTimeTablesAndGetTriggers(index, v._1, m, v._2)
          if (trigger != None) {
            println(s"TRIGGER!!!!! $trigger --> partition $index")
            // 产生了 trigger 信号, 说明要检查输出了..., 遍历每个 key
            newStateMap.getAll().foreach { case (key1, _, _) =>
              wrappedState.wrap(newStateMap.get(key1))
              // 暂时盗窃一下 timeout 的方法, 把 value 是 None 的情况视作是 global_trigger 信号到达的信息
              // 可以前往 StateJoinUtils 里提取最新的 global_trigger 信号
              val returned = mappingFunction(index, batchTime, key1, None, wrappedState)
              if (wrappedState.isRemoved) {
                newStateMap.remove(key1)
              } else if (wrappedState.isUpdated
                || (wrappedState.exists && timeoutThresholdTime.isDefined)) {
                newStateMap.put(key1, wrappedState.get(), batchTime.milliseconds)
              }
              mappedData ++= returned
            }
          }
        }
        case true => {
          // input1
          wrappedState.wrap(newStateMap.get(key))
          val returned = mappingFunction(index, batchTime, key, Some(value), wrappedState)
          if (wrappedState.isRemoved) {
            newStateMap.remove(key)
          } else if (wrappedState.isUpdated
            || (wrappedState.exists && timeoutThresholdTime.isDefined)) {
            newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
          }
          mappedData ++= returned
        }
      }
    }

    // Get the timed out state records, call the mapping function on each and collect the
    // data returned
    if (removeTimedoutData && timeoutThresholdTime.isDefined) {
      newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
        wrappedState.wrapTimingOutState(state)
        val returned = mappingFunction(index, batchTime, key, None, wrappedState)
        mappedData ++= returned
        newStateMap.remove(key)
      }
    }

    MyMapWithStateWithIndexRDDRecord(newStateMap, mappedData)
  }
}

/**
  * Partition of the [[MyMapWithStateWithIndexRDD]], which depends on corresponding partitions of prev state
  * RDD, and a partitioned keyed-data RDD
  */
private[streaming] class MyMapWithStateWithIndexRDDPartition(
                                                              override val index: Int,
                                                              @transient private var prevStateRDD: RDD[_],
                                                              @transient private var partitionedDataRDD: RDD[_]) extends Partition {

  private[rdd] var previousSessionRDDPartition: Partition = null
  private[rdd] var partitionedDataRDDPartition: Partition = null

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = other match {
    case that: MyMapWithStateWithIndexRDDPartition => index == that.index
    case _ => false
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    previousSessionRDDPartition = prevStateRDD.partitions(index)
    partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    oos.defaultWriteObject()
  }
}


/**
  * RDD storing the keyed states of `mapWithState` operation and corresponding mapped data.
  * Each partition of this RDD has a single record of type [[MyMapWithStateWithIndexRDDRecord]]. This contains a
  * [[StateMap]] (containing the keyed-states) and the sequence of records returned by the mapping
  * function of  `mapWithState`.
  *
  * @param prevStateRDD         The previous MyMapWithStateWithIndexRDD on whose StateMap data `this` RDD
  *                             will be created
  * @param partitionedDataRDD   The partitioned data RDD which is used update the previous StateMaps
  *                             in the `prevStateRDD` to create `this` RDD
  * @param mappingFunction      The function that will be used to update state and return new data
  * @param batchTime            The time of the batch to which this RDD belongs to. Use to update
  * @param timeoutThresholdTime The time to indicate which keys are timeout
  */
private[streaming] class MyMapWithStateWithIndexRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
                                                                                                         private var prevStateRDD: RDD[MyMapWithStateWithIndexRDDRecord[K, S, E]],
                                                                                                         private var partitionedDataRDD: RDD[(K, V)],
                                                                                                         mappingFunction: (Int, Time, K, Option[V], State[S]) => Option[E],
                                                                                                         batchTime: Time,
                                                                                                         timeoutThresholdTime: Option[Long],
                                                                                                         m: Int
                                                                                                       ) extends RDD[MyMapWithStateWithIndexRDDRecord[K, S, E]](
  partitionedDataRDD.sparkContext,
  List(
    new OneToOneDependency[MyMapWithStateWithIndexRDDRecord[K, S, E]](prevStateRDD),
    new OneToOneDependency(partitionedDataRDD))
) {

  @volatile private var doFullScan = false

  require(prevStateRDD.partitioner.nonEmpty)
  require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

  override val partitioner = prevStateRDD.partitioner

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def compute(
                        partition: Partition, context: TaskContext): Iterator[MyMapWithStateWithIndexRDDRecord[K, S, E]] = {

    val stateRDDPartition = partition.asInstanceOf[MyMapWithStateWithIndexRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)

    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next()) else None
    val newRecord = MyMapWithStateWithIndexRDDRecord.updateRecordWithData(
      m,
      stateRDDPartition.index,
      prevRecord,
      dataIterator,
      mappingFunction,
      batchTime,
      timeoutThresholdTime,
      removeTimedoutData = doFullScan // remove timedout data only when full scan is enabled
    )
    Iterator(newRecord)
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new MyMapWithStateWithIndexRDDPartition(i, prevStateRDD, partitionedDataRDD)
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }

  def setFullScan(): Unit = {
    doFullScan = true
  }
}

private[streaming] object MyMapWithStateWithIndexRDD {

  def createFromPairRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
                                                                             pairRDD: RDD[(K, S)],
                                                                             partitioner: Partitioner,
                                                                             updateTime: Time,
                                                                             m: Int): MyMapWithStateWithIndexRDD[K, V, S, E] = {

    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime.milliseconds) }
      Iterator(MyMapWithStateWithIndexRDDRecord(stateMap, Seq.empty[E]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

    val noOpFunc = (index: Int, time: Time, key: K, value: Option[V], state: State[S]) => None

    new MyMapWithStateWithIndexRDD[K, V, S, E](
      stateRDD, emptyDataRDD, noOpFunc, updateTime, None, m)
  }

  def createFromRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
                                                                         rdd: RDD[(K, S, Long)],
                                                                         partitioner: Partitioner,
                                                                         updateTime: Time,
                                                                         m: Int): MyMapWithStateWithIndexRDD[K, V, S, E] = {

    val pairRDD = rdd.map { x => (x._1, (x._2, x._3)) }
    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, (state, updateTime)) =>
        stateMap.put(key, state, updateTime)
      }
      Iterator(MyMapWithStateWithIndexRDDRecord(stateMap, Seq.empty[E]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

    val noOpFunc = (index: Int, time: Time, key: K, value: Option[V], state: State[S]) => None

    new MyMapWithStateWithIndexRDD[K, V, S, E](
      stateRDD, emptyDataRDD, noOpFunc, updateTime, None, m)
  }
}
