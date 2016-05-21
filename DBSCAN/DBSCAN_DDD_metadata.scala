import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.dbscan.DBSCAN


object DBSCAN_DDD_metadata {

val log = LoggerFactory.getLogger(DBSCAN_DDD_metadata.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DBSCAN on DDD metadata").
	set("spark.kryoserializer.buffer.max", "1024") //in MB ; by default 64MB
    val sc = new SparkContext(conf)

	val eps = args(0).toFloat
	val minPoints = args(1).toInt
	val maxPointsPerPartition = args(2).toInt
	val src = args(3)
	val dest = args(4)
	val phenotypes_path = args(5) // "datasets/ddd/ddd3_ega-phenotypes.txt"

	log.info(s"EPS: $eps minPoints: $minPoints")

	val data = sc.objectFile[(String, org.apache.spark.mllib.linalg.Vector)](src)
	val effects = data.mapValues{ x => x.toArray.takeRight(4) }
	var samples = data.mapValues{ x => Vectors.dense(x.toArray.slice(0,3))}

	val nb_elements = samples.count.toInt

	val model = DBSCAN.train(
	  samples,
	  eps = eps,
	  minPoints = minPoints,
	  maxPointsPerPartition = maxPointsPerPartition)


	import org.apache.hadoop.fs._
	import java.io.BufferedOutputStream

	val fs = FileSystem.get(sc.hadoopConfiguration)

	class TextFile(output_path : String) {
	  val physical_file = fs.create(new Path(output_path))
	  val stream = new BufferedOutputStream(physical_file)
	  
	  def write(text : String) : Unit = {
	    stream.write(text.getBytes("UTF-8"))
	  }
	  
	  def close() : Unit = {
	    stream.close()
	  }
	}

	val t = new TextFile(dest)

	val cluster_by_name = model.labeledPoints.map(p => (p.id, p.cluster))


	val pheno_data = sc.textFile(phenotypes_path)
	val phenotypes = pheno_data.map(_.split('\t')).map{
	  x =>
	  val terms = x(4).split(";")
	  x(0) -> terms
	}

	var txt = ""

	val res = cluster_by_name.join(data).map{
	  case (patient, (cluster_id, vector)) =>
	  (cluster_id, vector.toArray.mkString(", ") + "\n")
	}.persist

	val nb_members = res.mapValues(x => 1).reduceByKey(_ + _)



	val tot = cluster_by_name.join(effects).map{
	  case (patient, (cluster_id, effect_vector)) =>
	  (cluster_id, effect_vector)
	}.
	reduceByKey((a,b) =>  (a zip b).map{ case (a_val, b_val) => a_val + b_val }).
	join(nb_members).mapValues{
	    case (array, nb_mem) =>
	    array.map( _ *1.0 / nb_mem)
	} //to get average number of each type (high, moderate, low, modifier) per patient inside that cluster

	tot.collect.foreach{
	  case (id, res) =>
	  txt = "Proportions of each mutations over cluster " + id + " :\n" + " HIGH: " + res(0) + "\n MODERATE:" + res(1)
	  txt += "\n LOW:" + res(2) + "\n MODIFIER:" + res(3) + "\n"
	  println(txt)
	  t.write(txt)
	}


	val avg_vector = cluster_by_name.join(data).map{
	  case (patient, (cluster_id, vector)) =>
	  (cluster_id, vector.toArray)
	}.
	reduceByKey((a,b) =>  (a zip b).map{ case (a_val, b_val) => a_val + b_val }).
	join(nb_members).mapValues{
	  case (vector, nb) =>
	  vector.map( _ / nb)
	}


	val phenos_by_cluster = cluster_by_name.join(phenotypes).values

	val counts_by_pheno = phenos_by_cluster.map{
	  case(cluster, pheno_list) => pheno_list.map(e => ((cluster, e), 1))
	}.flatMap{a => a.toList}.reduceByKey(_ + _).persist

	val counts_overall = counts_by_pheno.map{
	  case ((cluster, hpo), cnt) =>
	  (hpo, cnt)
	}.reduceByKey(_ + _)

	val counts_by_cluster_with_percentages = counts_by_pheno.map{
	  case ((cluster, hpo), cnt) =>
	  (hpo, (cluster, cnt))
	}.join(counts_overall).map{
	  case (hpo, ((cluster, cnt), tot)) =>
	  (cluster, (hpo, cnt, cnt*100.0/tot))
	}.groupByKey

	val patients_by_cluster = cluster_by_name.map{case (patient, cluster) => (cluster, patient)}.reduceByKey(_ + ", " + _)


	res.aggregateByKey("")(_ +_, _+_).join(nb_members).join(patients_by_cluster).join(avg_vector).join(counts_by_cluster_with_percentages).
	collect.foreach{
	  case (cluster_id, ((((ensemble, nb), patients_list), avg_vector), hpo_array)) =>
	  txt = "----- CLUSTER " + cluster_id + " -----\n"
	  txt += "Total of " + nb + " patients :" + patients_list.toString + "\n"
	  txt += "Average vector: " + avg_vector.mkString(",") + "\n"
	  txt += ensemble
	  println(txt)
	  t.write(txt)
	  hpo_array.foreach{
	    case (hpo, cnt, perc) =>
	    val formatted_res = "%-40s: %d (%.1f%% of the ppl having this HPO)".format(hpo, cnt, perc)
	    println(formatted_res)
	    t.write(formatted_res + "\n")
	  }
	}

	t.close()
  }
}
