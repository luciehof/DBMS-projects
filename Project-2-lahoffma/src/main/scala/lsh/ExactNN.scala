package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold: Double) extends Construction with Serializable {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute exact near neighbors here
    rdd.zipWithUniqueId().cartesian(data) //TODO: there exists more efficient than cartesian
      .map(v => {
        val queryMovie = v._1._1._1
        val queryKeywords = v._1._1._2
        val queryIdx = v._1._2
        val dataMovie = v._2._1
        val dataKeywords = v._2._2

        if (jaccard(queryKeywords.toSet, dataKeywords.toSet) > threshold) (queryIdx,(queryMovie, Set[String](dataMovie))) else (queryIdx,(queryMovie, Set[String]()))
      })
      .reduceByKey((v1,v2) => {
        val movie = v1._1
        val set1 = v1._2
        val set2 = v2._2
        (movie, set1 union set2)
      })
      .values
  }

  def jaccard(s1: Set[String], s2: Set[String]): Double = {
    val unionSize = (s2 union s1).size
    if (unionSize>0) (s1 intersect s2).size.toFloat / unionSize
    else 1d
  }
}
