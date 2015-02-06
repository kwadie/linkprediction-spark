package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File

/**
 * Hello world!
 *
 */
object FeaturesCalc2 {

  def main(args: Array[String]) {
	val logFile = new File("log.txt")
    val log = new java.io.PrintWriter(logFile)

    //setting the environment
    val subgraphPath = "C:/tmp/spark/out/edges.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //spark Util to construct a graph from adjacency list 
    val graph = GraphLoader.edgeListFile(sc, subgraphPath)

    //get in+out degrees of nodes
    val degreesRDD: RDD[(Long, Int)] = graph.degrees.map(x => (x._1, x._2))

    val instancesPath = "C:/tmp/spark/out/sample.txt"
    var solSet: RDD[FeatureList] = sc.textFile(instancesPath, 4)
      .map { _.split(',') }
      .map {split => new FeatureList(split(0).toLong, split(1).toLong, split(2)) }

    //create an RDD with v as key and current feature list as value and join it with degrees to comput v degree
    var solSetRDD_V: RDD[(Long, FeatureList)] = solSet.map { x => (x.v, x) }
    solSet = solSetRDD_V.join(degreesRDD).map(f => f._2._1.setVdegree(f._2._2))
    log.println("v degree " + solSet.count())

    
    //do the same with u
    val solSetRDD_U: RDD[(Long, FeatureList)] = solSet.map { x => (x.u, x) }
    solSet = solSetRDD_U.join(degreesRDD).map(f => f._2._1.setUdegree(f._2._2))
    log.println("u degree " + solSet.count())
    
    //prepare and RDD of edges with edg.src as key (to be used in joins)
    val edgesRDD: RDD[(Long, Long)] = graph.edges.map { x => (x.srcId, x.dstId) }

    /*
     *  Total friends: distinct number of outgoing neigbours for v and u combined
     *  for each v,u entry omit 2 elements (VertexId, FeatureList) and join them 
     *  with directed edges on the rc attribute (first attribute) to give 
     *  ( VertexID, (FeatureList , edge.DestinationID) )
     *  then map the result to get  (FeatureList, DestinationID) =  for each group 
     *  key (FeatureList) with a neigbour node of either u or v from the original entry
     *  then group by key (FeatureList) (or v,u pair) and get the distinct number of 
     *  neigbour nodes, this is the total frirend measure 
     */
    solSet = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(edgesRDD).map(x => (x._2._1, x._2._2))
      .groupByKey().mapValues(_.toSet.size)
      .map(f => f._1.setTotalFriendsOut(f._2))

    log.println("total friends " + solSet.count())

    /*
     * Same as total friends but ommit ((FL,DestinatinID), 1) then reduce by key (FL,Des)
     * Then change the key to FL only and count how many destinationIDs appeared more than 
     * once in each group (FL) , these are the common friends
     */
    solSet = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(edgesRDD).map(f => (f._2, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1._1, x._2))
      .groupByKey()
      .mapValues(x => x.filter(_ > 1).size)
      .map(f => f._1.setCommonFriendsOut(f._2))
    log.println("total friends out " + solSet.count())

//    solSet.foreach(println(_))

    val output2 = new File("C:/tmp/spark/out/features.txt")
    val arrayFeatures = solSet.toArray.map(_.asCsv)
    printToFile(output2) { p => arrayFeatures.foreach(p.println) }

    log.close()
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}
