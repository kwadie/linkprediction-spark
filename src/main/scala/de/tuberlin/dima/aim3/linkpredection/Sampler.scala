package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Sampler {

  val SEED = 0x3133F

  val POS = 'Y'
  val NEG = 'N'

  def main(args: Array[String]) {
    val inputPath = "c:/tmp/data/slashdot0902/Slashdot0902.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, inputPath)
    val selectedVertices = graph.vertices.sample(false, 0.20, SEED)
    			.map { case (vid, atr) => vid.toInt }
    			.take(25000)
   
    val verticesBroadcasted = sc.broadcast(selectedVertices.toSet)

    val subgraph = graph.subgraph(vpred = (id, attr) => verticesBroadcasted.value.contains(id.toInt) )
    println(subgraph.vertices.count)
    
    val edges1 = subgraph.edges.map(e => (e.dstId, e))
    val edges2 = subgraph.edges.map(e => (e.srcId, e))
    val hardNetagives = edges1.join(edges2)
      .filter { case (_, (e1, e2)) => e1.srcId != e2.dstId }
      .map { case (_, (e1, e2)) => (e1.srcId, e2.dstId, NEG) }

    val edgeIds = subgraph.edges.map { case Edge(v1, v2, w) => (v1, v2) }

    val vertices = subgraph.vertices.sample(false, 0.1, SEED).map(v => v._1)
    val easyNegatives = vertices.cartesian(vertices)
      //.subtract(edgeIds) 
      // takes too long, we can just assume that it's highly unlikely to get a POS here  
      .sample(false, 0.005, SEED)
      .map { case (v1, v2) => (v1, v2, NEG) }

    hardNetagives.take(1000).union(easyNegatives.take(1000)).foreach(println(_))

  }
}


