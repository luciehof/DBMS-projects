package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.control.Breaks.{break, breakable}

class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int, partitions: Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  private val hashData = minHash.execute(data)
  private val buckets = hashData.map(v => (v._2, v._1)).aggregateByKey(Set[String]())((set, string) => set + string, (s1, s2) => s2 union s1)

  //partitions = nb workers we want to have
  // 1) take data to build buckets
  // 2) implement method for computing histogram of minhashes for queries
  // 3) implement method computing partition boundaries for histogram
  // 4) evl: use 2 methods to perform load balancing logic explained in slides
  def computeMinHashHistogram(queries: RDD[(String, Int)]): Array[(Int, Int)] = {
    //compute histogram for target buckets of queries
    queries.map(v => (v._2, v._1)).aggregateByKey(0)((s, _) => s + 1, (s1, s2) => s1 + s2).sortByKey().collect()
  }

  def computePartitions(histogram: Array[(Int, Int)]): Array[Int] = {
    //compute the boundaries of bucket partitions
    val threshold = math.ceil(histogram.length / partitions).toInt
    //var histIdx = 0
    var currPartSize = 0
    var partArray = Array[Int](0)
    histogram.zipWithIndex.foreach(h => {
      val count = h._1._2
      val index = h._2
      currPartSize += count
      if (currPartSize >= threshold && index != histogram.length-1) {
        partArray = partArray :+ index+1
        currPartSize = 0
      }
    })
    /*breakable {
      histogram.foreach(q => {
        currPartSize += q._2
        histIdx += 1
        if (currPartSize >= threshold) {
          if (histogram.length - 1 - histIdx < 2 * threshold) {
            partArray = partArray :+ (histogram.length - 1)
            break
          }
          partArray = partArray :+ histIdx
          currPartSize = 0
        }
      })
    }*/
    partArray
  }

  def pid(histo: Array[(Int, Int)], partArray: Array[Int]): Map[Int, Int] = {
    // get index of partArray s. t. partArray(index-1)<bid<=partArray(index)
    //val firstPartBound = partArray.head
    var partIdx = 0
    var bidToPid = Map[Int, Int]()
    histo.foreach(v => {
      val bid = v._1
      if (partIdx == partArray.length-1 || bid < partArray(partIdx+1)) {
        bidToPid = bidToPid + (bid -> partIdx)
      } else {
        partIdx += 1
        bidToPid = bidToPid + (bid -> partIdx)
      }
    })
    bidToPid
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here

    val hashQueries = minHash.execute(queries)
    var histo = Array[(Int, Int)]()
    var partArray = Array[Int]()
    var bidToPid = Map[Int, Int]()
    if (!queries.isEmpty()) {
      histo = computeMinHashHistogram(hashQueries)
      partArray = computePartitions(histo)
      bidToPid = pid(histo, partArray)
    }
    val partQueries = hashQueries.map(q => (bidToPid(q._2), q)).aggregateByKey(List[(String, Int)]())((l, q) => l :+ q, (l1, l2) => l1 ++ l2)
    val partBuckets = buckets.filter(b => bidToPid.contains(b._1)).map(b => (bidToPid(b._1), b)).aggregateByKey(List[(Int, Set[String])]())((l, q) => l :+ q, (l1, l2) => l1 ++ l2)
    partQueries.join(partBuckets).flatMap(v => {
      val queries = v._2._1
      val buckets = v._2._2
      queries.map(q => (q._1, buckets.filter(_._1 == q._2).head._2))
    })
  }
}