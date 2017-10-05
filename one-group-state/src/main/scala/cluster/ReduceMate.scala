package cluster

import scala.collection.mutable

/**
  * Created by kawhi on 26/09/2017.
  */
object ReduceMate {
  var globalSumS = Map[BigInt, Int]()
  var globalSumE = Map[BigInt, Int]()
  var dispersionS = 0
  var dispersionE = 0
}
