import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object kmeans_1000g_against_ddd {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("K-Means (1000G against DDD)")
    val sc = new SparkContext(conf)

  val hdfs_path = "hdfs:/user/ndewit/"
  val input_path = hdfs_path + "aligned_both_variants"
  val output_path = hdfs_path + "1000G_vs_DDD_Kmeans_results.txt"
  
  val numClusters = if (args.length > 0) { args(0).toInt } else { 2 }
  val numIterations = 20
  val numRuns = 1
  
  val outliers_limit = if (args.length > 1) { args(1).toInt } else { 0 }

  /* ... new cell ... */

  import org.apache.spark.mllib.linalg.Vectors
  var samples = sc.objectFile[(String, org.apache.spark.mllib.linalg.Vector)](input_path)
  var nb_samples = samples.count

  /* ... new cell ... */

  import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
  
  var check_outliers = Array[String]()
  var nb_outliers = check_outliers.size
  var predicted_clusters = sc.parallelize(Array[(String, Int)]())
  
  do {
  
    val values_only = samples.values
  
    val kmeans_model = KMeans.train(values_only, numClusters, numIterations, numRuns)
  
    predicted_clusters = samples.mapValues{kmeans_model.predict(_)}.persist
  
    check_outliers = predicted_clusters.
    map{ case (patient, cluster) => (cluster, patient) }.
    aggregateByKey(scala.collection.mutable.HashSet.empty[String])(_+_, _++_).values.
    flatMap{
      v =>
      if (v.size > outliers_limit) { List("") }
      else { v.toList }
    }.collect.filter(v => v != "")
    nb_outliers = check_outliers.size
  
    samples = samples.filter(s => !check_outliers.contains(s._1))
    nb_samples = samples.count
  
    println(nb_outliers + " outliers removed " +
            "(" + check_outliers.mkString(", ") + ") " +
            ": " + nb_samples + " samples remaining.")
  
  } while (nb_outliers > 0 && nb_samples > numClusters)

  /* ... new cell ... */

  import org.apache.hadoop.fs._
  import java.io.BufferedOutputStream
  
  val fs = FileSystem.get(sc.hadoopConfiguration)
  
  class TextFile(file_path : String) {
    val physical_file = fs.create(new Path(file_path))
    val stream = new BufferedOutputStream(physical_file)
  
    def write(text : String) : Unit = {
      stream.write(text.getBytes("UTF-8"))
    }
  
    def close() : Unit = {
      stream.close()
    }
  }
  
  val t = new TextFile(output_path)

  /* ... new cell ... */

  def RID(to_eval : RDD[((String, (Int, Int)), (String, (Int, Int)))]) : Double = {
  
    def choose2(n : Int) : Double = {
      return n * (n - 1) / 2;
    }
  
    val denom = choose2(nb_samples.toInt)  //Denominator of RID is (nb_samples choose 2)
  
    // a : number of pairs in the same cluster in C and in K
    // b : number of pairs in different clusters in C and in K
    val a = sc.accumulator(0, "Acc a : same cluster in both")
    val b = sc.accumulator(0, "Acc b : different cluster in both")
  
    to_eval.foreach{
      case ((id1, classes1), (id2, classes2)) =>
  
      if (id1 != id2) {
        if (classes1._1 == classes2._1 && classes1._2 == classes2._2) {
          a += 1 //Classes match, and they should
        }
        else if (classes1._1 != classes2._1 && classes1._2 != classes2._2) {
          b += 1 //Classes don't match, and they shouldn't
        }
      }
    }
  
    //We divide these counts by two since each pair was counted in both orders (a,b and b,a)
    (a.value/2 + b.value/2) / denom
  }
  
  val both = predicted_clusters.map{
  case (k, predicted) =>
  var label = 0
  if (k.slice(0,3) == "DDD") { label = 1 }
  (k, (label, predicted))
  }
  
  val eval_res = RID(both.cartesian(both))
  val txt = s"RID = $eval_res"
  println(txt)
  t.write(txt + "\n")
  
  predicted_clusters.map{case(a,b) => (b,a)}.
  groupByKey.collect.foreach{
    case (cluster, list_patients) =>
    println("--- Cluster " + cluster.toString + " ---")
    t.write("--- Cluster " + cluster.toString + " ---\n")  
    list_patients.foreach{
      p =>
      println(p)
      t.write(p + "\n")  
    }
  }

  /* ... new cell ... */

  t.close()

  }
}
