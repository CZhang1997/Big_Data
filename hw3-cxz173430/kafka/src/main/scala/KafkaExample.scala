import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

//import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

import com.google.common.io.Files
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraph
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.{BasicDependenciesAnnotation}
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.trees.Tree

import scala.collection.JavaConverters._

object KafkaExample {
  def getSentiment(sentiment: Int): String = sentiment match {
    case x if x == 0 || x == 1 => "Negative"
    case 2 => "Neutral"
    case x if x == 3 || x == 4 => "Positive"
  }
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Correct usage: Program_Name inputTopic")
      System.exit(1)
    }
    val filters = args.takeRight(args.length - 1)

    val consumerKey = "NG4AaraROFTYZGFS9C9PresYA";
    val consumerSecret = "AVMTbUe69kuep2CY53QGSZ0uJiLyT6FgkS1KhUSJ4ehtiOXYr8";
    val accessToken = "1130549159465869312-J6XcLjiNqtmUqGFw2xYfg4z8YpYM35";
    val accessTokenSecret = "wZrg78Zab5kZJ3biYifehqMdF8AGWaBPbSBlJAyKXb0X9";

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = args(0)

    val props2: Properties = new Properties()
    props2.put("annotators", "tokenize, ssplit, parse, sentiment")

    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props2)
    val texts = stream.map(status => status.getText());

    texts.foreachRDD( rdd => {
      rdd.collect().map(text =>{
        val document: Annotation = new Annotation(text)
        pipeline.annotate(document)
        val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList
        val ret = sentences
          .map(sentence => (sentence, sentence.get(classOf[SentimentAnnotatedTree])))
          .map { case (sentence, tree) => (sentence.toString, getSentiment(RNNCoreAnnotations.getPredictedClass(tree)), RNNCoreAnnotations.getPredictedClass(tree)) }
//
         try {
           ret.map(item => {
             val record = new ProducerRecord[String, String](topic, "{\n\"tweets\": %s,\n\"analyz\": %s,\n\"value\":%d\n}".format(item._1, item._2, item._3))
             val metadata = producer.send(record)
             printf(s"sent record(key=%s value=%s) " +
               "meta(partition=%d, offset=%d)\n",
               record.key(), record.value(),
               metadata.get().partition(),
               metadata.get().offset())

           })}
         catch{
        case e:Exception => e.printStackTrace()
             }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
