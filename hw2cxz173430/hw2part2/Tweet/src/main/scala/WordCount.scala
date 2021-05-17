import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer, HashingTF, StringIndexer, IDF}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Tweet {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: WordCount InputDir OutputDir")
    }
    val input = args(0)
    val output = args(1)

    val spark = SparkSession
      .builder()
      .appName("Page Rank")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.option("header","true").option("inferSchema","true").csv(input).select("text", "airline_sentiment").na.drop(Seq("text"))
    val Array(train, test) = df.randomSplit(Array(0.9, 0.1))

    // Preprocessing using PipeLine
    // Stage 1: Transform the text column into words by breaking down the sentence into words
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    // Stage 2: Remove stop-words from the text column
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("noStopWords")
    // Stage 3: Convert words to term-frequency vectors
    val hashingTF = new HashingTF().setInputCol("noStopWords").setOutputCol("rawFeatures")//.setNumFeatures(40)
    // Stage 4: normalize term-frequency
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // Stage 5: convert label(string) to numeric format
    val indexer = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label")
    // Stage 6: logic regression
    val lr = new LogisticRegression()

    // Pipleline
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, idf, indexer, lr))
    //val df_processed = pipeline.fit(df).transform(df).select("label", "features")

    // Use a ParamGridBuilder to construct a grid of parameters to search ove
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 50, 100))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.maxIter, Array(100, 500))
      .build()

    // Treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)  // Use 3+ in practice
//      .setParallelism(2)

    //.select("label", "prediction")

    // Run cross-validation, and choose the best set of parameters
    val cvModel = cv.fit(train)

    val df_result = cvModel.transform(test).select("label", "prediction")

    df_result.rdd.saveAsTextFile(output)

    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setLabelCol("label")
    evaluator.setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df_result)

  }

}
