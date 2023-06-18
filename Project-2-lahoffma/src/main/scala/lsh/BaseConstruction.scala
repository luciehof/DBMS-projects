package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int) extends Construction {
  //build buckets here
  val minHash = new MinHash(seed)
  private val hashData = minHash.execute(data)
  private val buckets = hashData.map(v => (v._2, v._1)).aggregateByKey(Set[String]())((set, string) => set + string, (s1, s2) => s2 union s1)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    // use minhash to retrieve matches of queries from corresponding bucket: JOIN
    val hashRdd = minHash.execute(queries)
    hashRdd.map(v => (v._2, v._1)).leftOuterJoin(buckets).map(v => {
      val movie = v._2._1
      val set = if (v._2._2.isDefined) v._2._2.get else Set[String]()
      (movie,set)
    })
  }
}
