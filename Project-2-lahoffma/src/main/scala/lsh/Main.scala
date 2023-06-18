package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File


object Main {
  def generate(sc: SparkContext, input_file: String, output_file: String, fraction: Double): Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble / x._3.toDouble, 1))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    val avg_recall = recall_vec._1 / recall_vec._2

    avg_recall
  }

  def precision(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble / x._3.toDouble, 1))
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    val avg_precision = precision_vec._1 / precision_vec._2

    avg_precision
  }

  def construction1(SQLContext: SQLContext, rdd_corpus: RDD[(String, List[String])]): Construction = {
    //implement construction1 composition here
    // we want precision(ground, res) > 0.94
    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath

    val sc = SparkContext.getOrCreate()
    val rdd_query_collect = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = sc.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))

    val exact = new ExactNN(SQLContext, rdd_corpus, 0.3)

    var seed = 0
    var constructions = List(new BaseConstructionBroadcast(SQLContext, rdd_corpus, seed))
    var andConstruction = new ANDConstruction(constructions)
    val ground = exact.eval(rdd_query)
    var res = andConstruction.eval(rdd_query)

    while (precision(ground, res) <= 0.94) {
      seed += 1
      constructions = constructions :+ new BaseConstructionBroadcast(SQLContext, rdd_corpus, seed)
      andConstruction = new ANDConstruction(constructions)
      res = andConstruction.eval(rdd_query)
    }
    andConstruction
  }

  def construction2(SQLContext: SQLContext, rdd_corpus: RDD[(String, List[String])]): Construction = {
    //implement construction2 composition here
    val query_file = new File(getClass.getResource("/queries-10-2.csv/part-00000").getFile).getPath
    val sc = SparkContext.getOrCreate()
    val rdd_query_collect = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = sc.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))

    val exact = new ExactNN(SQLContext, rdd_corpus, 0.3)

    var seed = 0
    var constructions = List(new BaseConstructionBroadcast(SQLContext, rdd_corpus, seed))
    var orConstruction = new ORConstruction(constructions)
    val ground = exact.eval(rdd_query)
    var res = orConstruction.eval(rdd_query)

    while (recall(ground, res) <= 0.95) {
      seed += 1
      constructions = constructions :+ new BaseConstructionBroadcast(SQLContext, rdd_corpus, seed)
      orConstruction = new ORConstruction(constructions)
      res = orConstruction.eval(rdd_query)
    }
    orConstruction
  }

  /*
  source for the following function: https://biercoff.com/easily-measuring-code-execution-time-in-scala/
   */
  def time[R](block: => R, function: String): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time for " + function + ": " + (t1 - t0) + "ns")
    result
  }

  def main(args: Array[String]) {
    //TODO: get execution time for LSH implementations
    // + average of averages distance of each query point from their nearest neighbors
    // and report the difference between the exact and approximate solutions
    // --> pairwise comparisons of datasets
    // (try the different datasets where X is the same,
    // for example corpus-10 with queries-10-2, with/without skew)
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val corpus_file_1 = new File(getClass.getResource("/corpus-1.csv/part-00000").getFile).getPath
    val corpus_file_10 = new File(getClass.getResource("/corpus-10.csv/part-00000").getFile).getPath
    val corpus_file_20 = new File(getClass.getResource("/corpus-20.csv/part-00000").getFile).getPath

    val rdd_corpus_1 = sc
      .textFile(corpus_file_1)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    val rdd_corpus_10 = sc
      .textFile(corpus_file_10)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    val rdd_corpus_20 = sc
      .textFile(corpus_file_20)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    //type your queries here
    val query_file = new File(getClass.getResource("/queries-1-10.csv/part-00000").getFile).getPath
    val rdd_query_collect = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .collect()

    val rdd_query = sc.parallelize(rdd_query_collect.slice(0, rdd_query_collect.size / 1000))
    val rdd_corpus = rdd_corpus_20

    val seed = 43
    val partitions = 16
    val threshold = 0.6
    val exact = new ExactNN(sqlContext, rdd_corpus, threshold)
    val baseConstruction = new BaseConstruction(sqlContext, rdd_corpus, seed)
    val baseConstructionBroadcast = new BaseConstructionBroadcast(sqlContext, rdd_corpus, seed)
    val baseConstructionBalanced = new BaseConstructionBalanced(sqlContext, rdd_corpus, seed, partitions)

    val nbQueries = rdd_query.count()

    val ground = time(exact.eval(rdd_query), "ground")
    val avgDistanceGround = getAvgDistance(rdd_query, ground, rdd_corpus, exact, nbQueries)
    println("avgDistanceGround: "+avgDistanceGround.toString)
    val resBC = time(baseConstruction.eval(rdd_query), "resBC")
    val avgDistanceBC = getAvgDistance(rdd_query, resBC, rdd_corpus, exact, nbQueries)
    println("avgDistanceBC: "+ avgDistanceBC.toString)
    val resBCBCST = time(baseConstructionBroadcast.eval(rdd_query), "resBCBCST")
    val avgDistanceBCBCST = getAvgDistance(rdd_query, resBCBCST, rdd_corpus, exact, nbQueries)
    println("avgDistanceBCBCST: "+ avgDistanceBCBCST.toString)
    val resBCBL = time(baseConstructionBalanced.eval(rdd_query), "resBCBL")
    val avgDistanceBCBL = getAvgDistance(rdd_query, resBCBL, rdd_corpus, exact, nbQueries)
    println("avgDistanceBCBL: "+avgDistanceBCBL.toString)

  }

  private def getAvgDistance(rdd_query: RDD[(String, List[String])], res: RDD[(String, Set[String])], rdd_corpus: RDD[(String, List[String])], exact: ExactNN, nbQueries: Long): Double = {
    rdd_query.zipWithIndex.map(_.swap).join(res.zipWithIndex.map(_.swap)).values.collect().map(v => {
    //rdd_query.leftOuterJoin(res).collect().map(v => {
      val queryKeywords = v._1._2.toSet
      val setMovies = v._2._2
      setMovies.aggregate(0d)((avg, movie) => {
        val dist = rdd_corpus.filter(_._1 == movie).values.map(ms => 1 - exact.jaccard(queryKeywords, ms.toSet)).collect().head //TODO: here we assume no duplicate movies in corpus
        //val dist = ds.sum() / ds.count().toFloat
        avg + dist / setMovies.size.toFloat
      }, (a1, a2) => a1 + a2)
    }).sum / nbQueries.toFloat
  }
}
