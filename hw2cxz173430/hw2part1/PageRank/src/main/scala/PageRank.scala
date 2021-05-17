import org.apache.spark.sql.SparkSession

object PageRank {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: Page Rank :  InputDir, Iteration, OutputDir")
    }
    val input = args(0)
    val iters = args(1)
    val output = args(2)

    val spark = SparkSession
      .builder()
      .appName("Page Rank")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val airPorts = spark.read.option("header","true").option("inferSchema","true").csv(input).drop("_c4")

    val locations = airPorts.drop("DEST").drop("DEST_CITY_NAME").rdd.distinct().map{
      cols =>
        (cols(0), cols(1))
    }

    val graph = airPorts.rdd.map{
      cols =>
        (cols(0), cols(2))
    }.groupByKey()

    var ranks = graph.mapValues(a => 10.0)
    val N = ranks.count()
    val a = 0.15

    for (i <- iters) {
      val contribs = graph.join(ranks).values.flatMap{ case (flights, rank) =>
        val C = flights.size
        flights.map(flight => (flight, rank / C))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(a / N + (1 - a) * _)
    }
    val sorted = ranks.join(locations).map{case (code, (rank, name)) => (code, name, rank)}.sortBy(-_._3)
    sorted.saveAsTextFile(output)
  }

}
