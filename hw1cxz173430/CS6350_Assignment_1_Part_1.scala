// Databricks notebook source
// runtime 6.4 
// part 1
// 4.1. Insatll New -> PyPI -> spark-nlp -> Install
// 4.2. Install New -> Maven -> Coordinates -> com.johnsnowlabs.nlp:spark-nlp_2.11:2.7.4 -> Install
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP


// COMMAND ----------

// install model
val model = "recognize_entities_dl"
val pipeline = PretrainedPipeline(model, lang="en")

// COMMAND ----------

val path = "/FileStore/tables/64607_0.txt"
val story = sc.textFile(path)

// COMMAND ----------

// val enti = "named_entity"
val enti = "entities"
// filter out all the names 
val names = story.filter(x => x.length > 0).flatMap(line => pipeline.annotate(line).get(enti).toList.head.filter(x => x.length > 4))
// val names2 = story.filter(x => x.length > 0).flatMap(line => pipeline2.annotate(line).get("entities").toList.head.filter(x => x.length > 4))

// COMMAND ----------

val namesMap = names.map(x => (x,1))
// val namesMap2 = names2.map(x => (x,1))
// namesMap.collect()

// COMMAND ----------

val namesCount = namesMap.reduceByKey((x,y) => x+y).sortBy(-_._2)
// val namesCount2 = namesMap2.reduceByKey((x,y) => x+y).sortBy(-_._2)

// COMMAND ----------

// part 1 answer
val results = namesCount.map{case(name, count) => name}

// COMMAND ----------

results.take(10)

// COMMAND ----------


