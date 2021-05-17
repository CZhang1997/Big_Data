// Databricks notebook source
// runtime 6.4 
// part 2
// 4.1. Insatll New -> PyPI -> spark-nlp -> Install
// 4.2. Install New -> Maven -> Coordinates -> com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.4 -> Install
// /FileStore/tables/plot_summaries.txt

// COMMAND ----------

import scala.math.{log}
import org.apache.spark.sql.SQLContext

val path = "/FileStore/tables/plot_summaries.txt"
val path2 = "/FileStore/tables/short_summaries.txt"
val plotlines = sc.textFile(path)
val numPattern = "[0-9]+".r

val stopwordsPath = "/FileStore/tables/stopwords.txt"
val stopwordsData = sc.textFile(stopwordsPath)
val stopwords = stopwordsData.filter(x => x.length > 0).flatMap(line => line.split("""\W+""")).map(w => (w, 1))

val movieNamePath = "/FileStore/tables/movie_metadata.tsv"


// COMMAND ----------

// MAGIC %scala 
// MAGIC val movie = spark.read.format("csv")
// MAGIC  .option("delimiter", "\t")
// MAGIC   .load(movieNamePath)

// COMMAND ----------

val movieRDD = movie.rdd.map(row => (row(0).asInstanceOf[String].toInt, row(2).asInstanceOf[String]))


// COMMAND ----------


def mapIdPlots(line: String):  scala.collection.immutable.Map[String,(Int, Int)]={
  // extract out the id
  val id = numPattern.findFirstIn(line)
  // get the plot data
  val plot = line.replace(id.get, "").toLowerCase()
  // do the word count map reduce
  val words = plot.split("""\W+""")
  val plotWords = words.map(w => (w, 1))
  val red = plotWords.groupBy(_._1)
    .map{ case (key, list) => key -> list.map(_._2).reduce(_+_) }
  val ret = red.map{case (x: String, y: Int) => (x -> (id.get.toInt, y))}
  return ret
}

def mapIdPlotsNormalize(line: String):  scala.collection.immutable.Map[String,(Int, Double)]={
  // extract out the id
  val id = numPattern.findFirstIn(line)
  // get the plot data
  val plot = line.replace(id.get, "").toLowerCase()
  // do the word count map reduce
  val words = plot.split("""\W+""")
  val plotWords = words.map(w => (w, 1))
  val totalWords = plotWords.length
  val red = plotWords.groupBy(_._1)
    .map{ case (key, list) => key -> list.map(_._2).reduce(_+_)}
  val ret = red.map{case (x: String, y: Int) => (x -> (id.get.toInt, y.toDouble / totalWords.toDouble))}
  return ret
}

def mapIdPlotData(line: String): (Int, String)={
  // extract out the id
  val id = numPattern.findFirstIn(line)
  // get the plot data
  val plot = line.replace(id.get, "").toLowerCase()
  
  return (id.get.toInt, plot)
}


// COMMAND ----------

// get number of doc as N
val N = plotlines.filter(x => x.length > 0).count()
// use for getting a plot using id
val plot_data = plotlines.filter(x => x.length > 0).map(mapIdPlotData)


// COMMAND ----------

// get number of doc as N
val N = plotlines.filter(x => x.length > 0).count()
// use for getting a plot using id
val plot_data = plotlines.filter(x => x.length > 0).map(mapIdPlotData)
// get the word counts for each doc
// will return (word, (id, count))
val plots = plotlines.filter(x => x.length > 0).flatMap(mapIdPlots)
// remove stop words
val plotsFiltered = plots.subtractByKey(stopwords)
// first group by key(word), then return a list of (documentID, word counts)
val plotsMap = plotsFiltered.groupBy(_._1).map{ case(word, pairs) => word -> pairs.map{case(word, value) => value}}
// calculate the tf idf weight for each word
val tf_idf_value = plotsMap.map{case(word, list) => word -> (list.map{case (id, count)=> (id, count*log(N.toDouble/list.size.toDouble))}.toList.sortBy(-_._2))}
// convert it to a map for single term search
val tf_idf_value_map = tf_idf_value.collect().toMap

// COMMAND ----------

// part 1 result, change searchWord to search for other words
val searchWord = "king"
// val ret = tfValue.get(searchWord).map(list => list.flatMap{case (id, value) => id -> value})
val ret = tf_idf_value_map.get(searchWord).map(list => list.map{case (id, value) => id}.take(10)).toList.head.take(10)
val names = ret.map(id => idMapName.get(id).getOrElse(""))


// COMMAND ----------

names

// COMMAND ----------

// will return (word, (id, normalize value))
val plots_normalize = plotlines.filter(x => x.length > 0).flatMap(mapIdPlotsNormalize)
// remove stop words
val plots_nor_filtered = plots_normalize.subtractByKey(stopwords)
// first group by key(word), then return a list of (documentID, word counts)
val plotsMapNormalize = plots_nor_filtered.groupBy(_._1).map{ case(word, pairs) => word -> pairs.map{case(word, value) => value}}
val tf_idf_value_normalize = plotsMapNormalize.map{case(word, list) => word -> (list.map{case (id, count)=> (id, count * (1+log(N.toDouble/list.size.toDouble)))}.toList.sortBy(-_._2))}
// get the idf value for each words
val idf_value = plotsMapNormalize.map{case(word, list) => word -> (1.0 + log(N.toDouble/list.size.toDouble))}

// COMMAND ----------

// part 2 query, change query to search for other query
val query = "i am home school" 
// mapreduce the query
val wordsCount = query.split("""\W+""").map(w => (w.toLowerCase(),1))
val wordRDD = sc.parallelize(wordsCount)
val queryCount = wordRDD.reduceByKey((x, y) => x+y)
// find the tf idf for the query
val queryidf_tf = queryCount.join(idf_value).map{case(word, pair) => word -> pair._1*pair._2}
val queryTotal = queryidf_tf.map{case(word, value) => value*value}.sum
// find the dot product and ||document||
val dotProductsAndTotal = queryidf_tf.join(tf_idf_value_normalize).flatMap{case(word, value) => value._2.map{case(id, tf) => id -> (value._1 * tf, tf) }}.reduceByKey{case(x,y) => (x._1+y._1, x._2+y._2)}
// find similarity and sort by it
val similarity = dotProductsAndTotal.map{case(id, value) => id ->value._1 / (queryTotal* value._2)}.sortBy(-_._2)

// COMMAND ----------

// result
val top10DocForQuery = similarity.join(movieRDD).map{case(id, value) => id -> value._2}.take(10)

// COMMAND ----------

top10DocForQuery.map(_._2)


// COMMAND ----------


