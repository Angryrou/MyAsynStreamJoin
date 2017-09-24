package cluster

/**
  * Created by kawhi on 22/09/2017.
  */
object DMate {
  var lambda = 0.0 // maximum load / dispersion
  var m = 15 // mapper size
  var M = 150000 * 3
  private val delta = 0.0d to 10.0d by 0.1d toArray // delta
  private val h_delta = delta.map(x => (1+x) * Math.log(1+x) - x) // h_delta

  var p1 = -0.1 // biggest possibility of z
  var k = 0 // key size

//  private def getDelta():Double = {
//    val h = 2 * p1 * m * Math.log(m)
//    var index = 0
//    val iter = h_delta.toIterator
//    while(iter.hasNext) {
//      val a = iter.next() // h(index)
//      if (a < h) {
//        index += 1
//      }
//      else {
//        return delta(index)
//      }
//    }
//    return delta(index)
//  }
//
//  def getStrategy():Int = {
////    val delta = getDelta()
////    val costHH = (1+delta) / m
////    val costRR = 1.0 / m + lambda * k * (m - 1)
////    val costPK = (if (m < 2 / p1) 1.0 / m else costHH / 2) + lambda * (m - 1)
//
//    //    val costHH = (1 * 0.6 + delta * 0.4) * M / m
//    val costHH = (1 + 13 * p1) * M / m
//    val costRR = 1.0 * M / m + lambda * k * (m - 1)
//    //    val costPK = (if (m < 2 / p1) M * 1.0 / m else (1 * 0.6+delta * 0.4) / 2 / m) * M + lambda * k
//    val costPK = (if (m < 2 / p1) M * 1.0 / m else (1 + 13 * p1) * M / m / 2) + lambda * k
//
//    // 0 for HH, 1 for PK, 2 for RR
//    val ret = if (costHH <= costPK && costHH <= costPK) 0 else if (costPK <= costRR) 1 else 2
//
//    println(s"p1: $p1, costHH: $costHH, costPK: $costPK, cost RR: $costRR strategy: $ret")
//    ret
//  }
}