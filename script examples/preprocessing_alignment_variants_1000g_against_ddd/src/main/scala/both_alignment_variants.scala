import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object both_alignment_variants {
  def main(args: Array[String]) {
    val conf = new SparkConf().
	setAppName("Preprocessing - Alignment of variants (1000G against DDD)")
    val sc = new SparkContext(conf)

val nb_patients = if (args.length > 0) { args(0).toInt } else { 50 }
val reject_list = if (args.length > 1) { args(1).split(";") } else { Array("") }

  val pathVariants_1000g = "/user/hive/warehouse/1000g.db/exomes_1000g"
  val pathVariants_ddd = "/user/hive/warehouse/1000g.db/ddd"
  val ddd_families_path = "datasets/ddd/ddd3_family_relationships.txt"
  val hdfs_path = "hdfs:/user/ndewit/"
  val output_path = hdfs_path + "aligned_both_variants"

  /* ... new cell ... */

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  sqlContext.sql("SET spark.sql.parquet.binaryAsString=true")
  import sqlContext.implicits._

  /* ... new cell ... */

  /*
  val gene_list = List("datasets/ddd/ASD_genes",
                        "datasets/ddd/DDD_genes",
                        "datasets/ddd/ID_genes").
  map(path => sc.textFile(path)).
  reduce(_ union _).
  toDF("gene_list").
  distinct
  */

  /* ... new cell ... */

  var parquetFile = sqlContext.read.parquet(pathVariants_ddd)
  parquetFile.registerTempTable("dddData")
  
  val source_data = sc.textFile(ddd_families_path)
  val families = source_data.map(_.split('\t')).filter(_(2) != "0").map(x => (x(1), x(2), x(3)))
  
  val children_ids = families.map(_._1)
  
  val patients_id_ddd = children_ids.take(nb_patients)

  /* ... new cell ... */

  def make_request_ddd(cols : String) : String = {
    var request = "SELECT " + cols + " "
    request += "FROM dddData "
    request += "WHERE ("
    request += "filters = 'PASS' "
    request += "AND allele_num <= 2 "
    request += "AND gene_symbol IS NOT NULL "
    //request += "AND consensus_maf < 0.01"
    request += "AND chr = 22"
    request += ")"
    return request
  }
  
  import org.apache.spark.sql.functions.lit
  
  val initial_by_patient_ddd = sqlContext.
  sql(make_request_ddd("patient, pos, alternative")).
  where($"patient".isin(patients_id_ddd.map(lit(_)):_*)).
  where(!$"patient".isin(reject_list.map(lit(_)):_*)).
  //join(gene_list, $"gene_symbol" === $"gene_list").
  //drop("gene_symbol").
  map{ row => (row.getString(0), (row.getInt(1), row.getString(2))) }.
  aggregateByKey(scala.collection.mutable.HashSet.empty[(Int, String)])(_+_, _++_).
  mapValues(_.toArray)

  /* ... new cell ... */

  val all_pos_ddd = sqlContext.
  sql(make_request_ddd("patient, pos")).
  where($"patient".isin(patients_id_ddd.map(lit(_)):_*)).
  where(!$"patient".isin(reject_list.map(lit(_)):_*)).
  //join(gene_list, $"gene_symbol" === $"gene_list").
  select("pos")

  /* ... new cell ... */

  val parquetFile2 = sqlContext.read.parquet(pathVariants_1000g)
  parquetFile2.registerTempTable("thousandGData")

  /* ... new cell ... */

  val patients_id_1000G = sqlContext.
  sql("SELECT DISTINCT patient FROM thousandGData " +
      "LIMIT " + nb_patients.toString).
  map(_.getString(0)).collect

  /* ... new cell ... */

  def make_request_1000G(cols : String) : String = {
    var request = "SELECT " + cols + " "
    request += "FROM thousandGData "
    request += "WHERE ("
    request += "filters = 'PASS' "
    request += "AND allele_num <= 2 "
    request += "AND gene_symbol IS NOT NULL "
    //request += "AND consensus_maf < 0.01"
    request += "AND chr = 22"
    request += ")"
    return request
  }
  
  import org.apache.spark.sql.functions.lit
  
  val initial_by_patient_1000G = sqlContext.
  sql(make_request_1000G("patient, pos, alternative")).
  where($"patient".isin(patients_id_1000G.map(lit(_)):_*)).
  where(!$"patient".isin(reject_list.map(lit(_)):_*)).
  //join(gene_list, $"gene_symbol" === $"gene_list").
  //drop("gene_symbol").
  map{ row => (row.getString(0), (row.getInt(1), row.getString(2))) }.
  aggregateByKey(scala.collection.mutable.HashSet.empty[(Int, String)])(_+_, _++_).
  mapValues(_.toArray)

  /* ... new cell ... */

  val all_pos_1000G = sqlContext.
  sql(make_request_1000G("patient, gene_symbol, pos")).
  where($"patient".isin(patients_id_1000G.map(lit(_)):_*)).
  where(!$"patient".isin(reject_list.map(lit(_)):_*)).
  //join(gene_list, $"gene_symbol" === $"gene_list").
  select("pos")

  /* ... new cell ... */

  val all_pos = all_pos_ddd.unionAll(all_pos_1000G).
  distinct.
  map(_.getInt(0)).map((0, _)).
  groupByKey.map(_._2).map(_.toArray)
  
  val patients_id = patients_id_ddd ++ patients_id_1000G
  
  val initial_by_patient = initial_by_patient_1000G.union(initial_by_patient_ddd)
  
  val all_pos_per_patient = sc.parallelize(patients_id).cartesian(all_pos)

  /* ... new cell ... */

  val samples = initial_by_patient.join(all_pos_per_patient).map{
  
    case (patient, (features, all_pos)) =>
  
    val pos = features.map(_._1) //list of positions present in the patient
  
    val not_appearing = all_pos.filter(!pos.contains(_)). //for all positions not appearing in the patient
    flatMap{
      int_pos =>
      val pos = BigDecimal(int_pos)
      Array((pos + BigDecimal(0.1), 0.0),
            (pos + BigDecimal(0.2), 0.0),
            (pos + BigDecimal(0.3), 0.0),
            (pos + BigDecimal(0.4), 0.0)
           ).toList
      //We add the four nucleotides as zeros
    }
  
    var features_four = features.flatMap{
      case (int_pos, alt) =>
      val pos = int_pos
      var a = 0.0
      var c = 0.0
      var t = 0.0
      var g = 0.0
      if (alt == "A") { a = 1.0 }
      else if (alt == "C") { c = 1.0 }
      else if (alt == "T") { t = 1.0 }
      else if (alt == "G") { g = 1.0 }
  
      Array (
        (pos + BigDecimal(0.1), a), 
        (pos + BigDecimal(0.2), c), 
        (pos + BigDecimal(0.3), t), 
        (pos + BigDecimal(0.4), g)
      ).toList
    }
  
    //Manipulation to avoid multiple alleles for one position: group variants by common position,
    //take only the first element of each resulting list (a rare but possible occurrece)
    features_four = features_four.groupBy(_._1).map{ case (pos, list) => (pos, list.apply(0)._2)}.toArray
  
    //Join both lists, order them by growing position and discard the positions to keep only the created features
    val ordered_array = features_four.union(not_appearing).sortBy(_._1).map(_._2)
  
    (patient, ordered_array)
  }.map{
    case (patient, features) =>
    (patient, org.apache.spark.mllib.linalg.Vectors.dense(features))
  }

  /* ... new cell ... */

  /*
  //Size = Alignment of all positions * 4 possible values for each
  samples.mapValues(_.size).collect.foreach{
    e =>
    println("Size of alignment for patient " + e._1 + ": " + e._2) 
  }
  */

  /* ... new cell ... */

  import org.apache.hadoop.fs._
  import java.net.URI
  val fs:FileSystem = FileSystem.get(new URI(output_path), sc.hadoopConfiguration)
  fs.delete(new Path(output_path), true) // "True" to activate recursive deletion of files if it is a folder
  
  samples.saveAsObjectFile(output_path)



  }
}
