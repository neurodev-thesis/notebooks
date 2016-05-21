import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.dbscan.DBSCAN


object DBSCAN_acute_infl {

val log = LoggerFactory.getLogger(DBSCAN_acute_infl.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DBSCAN on Acute Inflammations").
	set("spark.kryoserializer.buffer.max", "1024") //in MB ; by default 64MB
    val sc = new SparkContext(conf)

	val eps = args(0).toFloat
	val minPoints = args(1).toInt
	val maxPointsPerPartition = args(2).toInt
	val src = args(3)
	val dest = args(4)
	  
	log.info(s"EPS: $eps minPoints: $minPoints")

	val nb_patients = 120

	val raw_data = sc.parallelize(sc.textFile(src).take(nb_patients))

	val elements = raw_data.map{
	  line => 
	  
	  val split = line.split('\t')

	  val id = split(0)
		                    
	  val temperature = split(1).toDouble
	  val others = split.slice(2, 7).map{ v => if (v == "no") 0.0 else 1.0 }   
	  val res = Array(temperature) ++ others
	  
	  (id, Vectors.dense(res))
	}

	val phenotypes = raw_data.map{
	  line => 
	  
	  val split = line.split('\t')
		                    
	  val id = split(0)

	  val decisions = split.takeRight(2)
	  var label = 0
	  if (decisions(0) == "no" && decisions(1) == "no") {
	    label = 0 //"neither"
	  } else if (decisions(0) == "yes" && decisions(1) == "no") {
	    label = 1 //"inflammation"
	  } else if (decisions(0) == "no" && decisions(1) == "yes") {
	    label = 2 // "nephretis"
	  } else {
	    label = 3 //"both"
	  }

	  (id, label)
	}

	val nb_elements = elements.count.toInt


	import org.apache.spark.mllib.feature.StandardScaler
	val stdscaler = new StandardScaler(withMean = true, withStd = true).fit(elements.values)
	val samples = elements.mapValues{ stdscaler.transform(_) }.persist


	val model = DBSCAN.train(
	  samples,
	  eps = eps,
	  minPoints = minPoints,
	  maxPointsPerPartition = maxPointsPerPartition)


	val both = model.labeledPoints.map(p => (p.id, p.cluster)).join(phenotypes).map{case (k,v) => (k.toInt, v)}
	val to_eval = both.cartesian(both)


	def RID(to_eval : RDD[((Int, (Int, Int)), (Int, (Int, Int)))]) : Double = {

	  def choose2(n : Int) : Double = {
	    return n * (n - 1) / 2;
	  }

	  val denom = choose2(nb_elements)

	  // a : number of pairs in the same cluster in C and in K
	  // b : number of pairs in different clusters in C and in K
	  val a = sc.accumulator(0, "Acc a : same cluster in both")
	  val b = sc.accumulator(0, "Acc b : different cluster in both")

	  to_eval.foreach{
	    case ((id1, classes1), (id2, classes2)) =>

	    if (id1 != id2) {
	      if (classes1._1 == classes2._1 && classes1._2 == classes2._2) {
		a += 1
	      }
	      else if (classes1._1 != classes2._1 && classes1._2 != classes2._2) {
		b += 1
	      }
	    }
	  }

	  (a.value/2 + b.value/2) / denom
	}

	val eval_res = RID(to_eval)
	  
	val numClusters = model.labeledPoints.map(p => p.cluster).distinct.count.toInt

	println(s"RID = $eval_res | for nb_elements = $nb_elements & numClusters = $numClusters")



	val predictions = model.labeledPoints.map(p => (p.id, p.cluster))

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
	t.write("Patient ID,Cluster ID")

	predictions.collect.foreach{
	  case (id, cluster) =>
	  t.write(id + "," + cluster + "\n")
	}

	t.close()
	  
	sc.stop()

  }
}
