package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import scala.util.Random

object Sampler {

  val SEED = 0x3133F

  val POS = 'Y'
  val NEG = 'N'

  def main(args: Array[String]) {

    val inputPath = "c:/tmp/data/slashdot0902/Slashdot0902.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, inputPath)

    val selectedVertices = graph.vertices.sample(false, 0.25, SEED)
      .map { case (vid, atr) => vid.toInt }
      .take(25000)
    val verticesBroadcast = sc.broadcast(selectedVertices.toSet)

    val subgraph = graph.subgraph(
      epred = e => e.srcId != e.dstId,
      vpred = (id, attr) => verticesBroadcast.value.contains(id.toInt))

    val output1 = new File("C:/tmp/spark/out/edges.txt")
    val graphArray = subgraph.edges.toArray.map(t => t.srcId + "\t" + t.dstId)
    printToFile(output1) { p => graphArray.foreach(p.println) }

    val dstEdges = subgraph.edges.map(e => (e.dstId, e))
    val srcEdges = subgraph.edges.map(e => (e.srcId, e))
    val hardNegatives = dstEdges.join(srcEdges)
      .filter { case (_, (e1, e2)) => e1.srcId != e2.dstId }
      .map { case (_, (e1, e2)) => ((e1.srcId, e2.dstId), NEG) }

    val srcIdEdges = subgraph.edges.map { e => e.srcId }
    val dstIdEdges = subgraph.edges.map { e => e.dstId }
    
    val vertices = srcIdEdges.union(dstIdEdges).toArray

	val len = vertices.length
    val rand = new Random(SEED)

    val easyNegatives1 = 
      Seq.fill(5500)(rand.nextInt(len), rand.nextInt(len))
    	 .filter { case (idx1, idx2) => idx1 != idx2 }
         .map { case (idx1, idx2) => (vertices(idx1), vertices(idx2), NEG) }
    
    val len2 = selectedVertices.length
    val easyNegatives2 = 
      Seq.fill(3000)(rand.nextInt(len2), rand.nextInt(len2))
    	 .filter { case (idx1, idx2) => idx1 != idx2 }
         .map { case (idx1, idx2) => (selectedVertices(idx1), selectedVertices(idx2), NEG) }
    
    
    val positive = subgraph.edges.sample(true, 0.20, SEED)
      .map { case Edge(v1, v2, w) => ((v1, v2), POS) }

    val labels = easyNegatives1.take(3000)
      .union(easyNegatives2.take(2000))
      .union(hardNegatives.take(5000))
      .union(positive.take(10000))

    val output2 = new File("C:/tmp/spark/out/sample.txt")
    val arrayFeatures = labels.toArray.map(t => flatProduct(t).mkString(","))

    printToFile(output2) { p => arrayFeatures.foreach(p.println) }
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  // http://stackoverflow.com/questions/5289408/iterate-over-a-tuple
  def flatProduct(t: Product): Iterator[Any] = t.productIterator.flatMap {
    case p: Product => flatProduct(p)
    case x => Iterator(x)
  }
}


