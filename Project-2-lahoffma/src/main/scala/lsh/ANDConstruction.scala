package lsh

import org.apache.spark.rdd.RDD

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute AND construction results here
    children.map(construction =>
      construction.eval(rdd) // result = movie|set of movies
    ).reduce((rdd1, rdd2) => rdd1.sortByKey().zip(rdd2.sortByKey()).map(v => (v._1._1,v._1._2 intersect v._2._2)))
  }
}
