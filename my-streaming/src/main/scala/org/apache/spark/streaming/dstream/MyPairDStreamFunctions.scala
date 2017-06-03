package org.apache.spark.streaming.dstream

/**
  * Created by kawhi on 31/05/2017.
  */

import org.apache.spark.HashPartitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming._

import scala.reflect.ClassTag

/**
  * Extra functions available on DStream of (key, value) pairs through an implicit conversion.
  */
class MyPairDStreamFunctions[K, V](self: DStream[(K, V)])
                                (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K])
  extends Serializable {
  private[streaming] def ssc = self.ssc

  private[streaming] def sparkContext = self.context.sparkContext

  private[streaming] def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism) = {
    new HashPartitioner(numPartitions)
  }

  @Experimental
  def myMapWithStateWithIndex[StateType: ClassTag, MappedType: ClassTag](
                                                                 spec: MyStateSpecWithIndex[K, V, StateType, MappedType],
                                                                 m: Int
                                                               ): MyMapWithStateWithIndexDstream[K, V, StateType, MappedType] = {
    new MyMapWithStateWithIndexDstreamImpl[K, V, StateType, MappedType](
      self,
      spec.asInstanceOf[MyStateSpecWithIndexImpl[K, V, StateType, MappedType]],
      m
    )
  }

  private def keyClass: Class[_] = kt.runtimeClass

  private def valueClass: Class[_] = vt.runtimeClass
}


object MyPairDStreamFunctions {

  implicit def addMyPairDstreamFunctions[K, V](stream: DStream[(K, V)])
                                              (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null):
  MyPairDStreamFunctions[K, V] = { new MyPairDStreamFunctions[K, V](stream)}

}