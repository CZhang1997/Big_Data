import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.functions.desc
import java.io._

object WordCount {
  def main(args: Array[String]): Unit = {

//    if (args.length != 2) {
//      println("Usage: WordCount InputDir OutputDir")
//    }
    val input = "amazon_data_short.txt" // args(0)
    val output = "out.txt" // args(1)
    // create Spark context with Spark configuration
    val config = new SparkConf().setAppName("Spark Count")//.setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(config)
    val pw = new PrintWriter(new File(output ))
    val graph = GraphLoader.edgeListFile(sc, input)


    // a. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each
    val outD = graph.outDegrees.sortBy(-_._2).take(5)
    pw.write(" a. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each\n")
    val str = outD.mkString(" ")
    pw.write(str)
    // COMMAND ----------

    // b. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges
    val inD = graph.inDegrees.collect().sortBy(-_._2).take(5)
    pw.write("\n\nb. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges\n")
    pw.write(inD.mkString(" "))
    // COMMAND ----------

    // c. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values
    val ranks = graph.pageRank(0.0001).vertices.collect().sortBy(-_._2).take(5)
    pw.write("\n\nc. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values\n")
    pw.write(ranks.mkString(" "))
    // COMMAND ----------

    // d. Run the connected components algorithm on it and find the top 5 components with the largest number of nodes
    val cc = graph.connectedComponents().vertices.collect().sortBy(-_._2).take(5)
    pw.write("\n\nd. Run the connected components algorithm on it and find the top 5 components with the largest number of nodes\n")
    pw.write(cc.mkString(" "))
    // COMMAND ----------

    // e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count
    val tc = graph.triangleCount().vertices.collect().sortBy(-_._2).take(5)
    pw.write("\n\ne. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count\n")
    pw.write(tc.mkString(" "))

    pw.close
  }

}
