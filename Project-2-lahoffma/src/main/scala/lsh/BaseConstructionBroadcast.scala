package lsh

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed: Int) extends Construction with Serializable {
  //build buckets here
  val minHash = new MinHash(seed)
  val hashData = minHash.execute(data)
  val buckets = hashData.map(v => (v._2, v._1)).aggregateByKey(Set[String]())((set, string) => set + string, (s1, s2) => s2 union s1)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    // optimization: use spark functionalities to distribute load among all spark workers
    // broadcast buckets to all workers
    val sc = SparkContext.getOrCreate()
    val bdcstBucks = sc.broadcast(buckets.collect().toMap)
    val hashRdd = minHash.execute(queries)
    hashRdd.map(v => (v._1, bdcstBucks.value.getOrElse(v._2, Set[String]())))
  }
}
