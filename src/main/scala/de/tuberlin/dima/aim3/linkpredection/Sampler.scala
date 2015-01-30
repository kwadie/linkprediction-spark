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
    val verticesBroadcast = sc.broadcast(selectedVertices.toSet)

    val subgraph = graph.subgraph(
      epred = e => e.srcId != e.dstId,
      vpred = (id, attr) => verticesBroadcast.value.contains(id.toInt))
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

    val positive = subgraph.edges.sample(true, 0.1, SEED)
      .map { case Edge(v1, v2, w) => (v1, v2, POS) }

    val data = hardNetagives.take(5000)
      .union(easyNegatives.take(5000))
      .union(positive.take(10000))

    // val dataBroadcast = sc.broadcast(data.toSet)
    // data.foreach(println(_))

    println("Calculating common neightbours...")

    // follow same people
    // e1 ----> o <----- e2
    // select count(*) from edges e1, edges e2 
    // where e1.dstId = e2.dstId and e1.srcId != e2.srcId
    // group by e1.srcId, e2.srcId
    val dstEdges = subgraph.edges.map(e => (e.dstId, e))

    val commonOut = dstEdges.join(dstEdges)
      .filter { case (dstId, (e1, e2)) => e1.srcId != e2.srcId }
      .map { case (dstId, (e1, e2)) => ((e1.srcId, e2.srcId), 1) }
      .reduceByKey(_ + _)


    // followed by the same people
    // e1 <---- o ----> e2

    val srcEdges = subgraph.edges.map(e => (e.srcId, e))
    val commonIn = srcEdges.join(srcEdges)
      .filter { case (srcId, (e1, e2)) => e1.dstId != e2.dstId }
      .map { case (srcId, (e1, e2)) => ((e1.dstId, e2.dstId), 1) }
      .reduceByKey(_ + _)

      
    //    val cnts = commonFriedsCnt.map { case ((e1, e2), cnt) => (cnt, 1) }
    //    val countDistr = cnts.reduceByKey(_ + _)
    //    countDistr.foreach(println(_))

    //    data.map {
    //      case (e1, e2, cls) =>
    //        val fl = new FeatureList(e1, e2)
    //
    //    }

  }
}


