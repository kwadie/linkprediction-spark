package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import java.io.PrintWriter
import java.io.File

object FeaturesCalc2 {

  def main(args: Array[String]) {

    val inputPath = "C:/tmp/spark/out/edges-test.txt"
//    		val inputPath = "C:/tmp/spark/out/edges.txt"

	// setting the environment
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // spark Util to construct a graph from adjacency list 
    val graph = GraphLoader.edgeListFile(sc, inputPath)

    //get bi,in,out degrees of nodes to be used in Join
    //NOTE: only vertices with values exist in the RDD - do left join or SolSet++
    val degreesRDD: RDD[(Long, Int)] = graph.degrees.map(x => (x._1, x._2))
    val indegreesRDD: RDD[(Long, Int)] = graph.inDegrees.map(x => (x._1, x._2))
    val outdegreesRDD: RDD[(Long, Int)] = graph.outDegrees.map(x => (x._1, x._2))

    val alldegreesRDD = degreesRDD.leftOuterJoin(indegreesRDD)
      .mapValues(f => new VertexDegrees(f._1, f._2.getOrElse(0), f._1 - f._2.getOrElse(0)))

    println("original graph edges: " + graph.edges.count())

//    val instancesPath = "C:/tmp/spark/out/sample.txt"
    val instancesPath = "C:/tmp/spark/out/sample-test.txt"
    var solSet: RDD[FeatureList] = sc.textFile(instancesPath, 4)
      .map { _.split(',') }
      .map { split => new FeatureList(split(0).toLong, split(1).toLong, split(2)) }

    println("original sol set: " + solSet.count())

    //create an RDD with v as key and current feature list as value and join it with degrees to comput v degree
    var solSetRDD_V: RDD[(Long, FeatureList)] = solSet.map { x => (x.v, x) }

    //get v degrees
    solSet = solSetRDD_V.join(alldegreesRDD)
      .map { f =>
        f._2._1.setVdegree(f._2._2.degree)
          .setVindegree(f._2._2.indegree)
          .setVoutdegree(f._2._2.outdegree)
      }

    //do the same with u
    var solSetRDD_U: RDD[(Long, FeatureList)] = solSet.map { x => (x.u, x) }

    //get u degrees
    solSet = solSetRDD_U.join(alldegreesRDD)
      .map { f =>
        f._2._1.setUdegree(f._2._2.degree)
          .setUindegree(f._2._2.indegree)
          .setUoutdegree(f._2._2.outdegree)
      }

    //prepare and RDD of edges with edg.src as key (to be used in joins)
    val edgesRDD: RDD[(Long, Long)] = graph.edges.map { x => (x.srcId, x.dstId) }

    val reverseEdgesRDD: RDD[(Long, Long)] = graph.edges.reverse.map { x => (x.srcId, x.dstId) }

    /*
     *  Total friends: distinct number of outgoing neigbours for v and u combined
     *  for each v,u entry omit 2 elements (VertexId, FeatureList) and join them 
     *  with directed edges on the rc attribute (first attribute) to give 
     *  ( VertexID, (FeatureList , edge.DestinationID) )
     *  then map the result to get  (FeatureList, DestinationID) =  for each group key 
     *  (FeatureList) with a neigbour node of either u or v from the original entry
     *  then group by key (FeatureList) (or v,u pair) and get the distinct number of 
     *  neigbour nodes, this is the total friend measure 
     */

    //temp hold operations result to be merged into the solSet
    var opResult = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(edgesRDD)
      .map(x => (x._2._1, x._2._2)).groupByKey()
      .mapValues(_.toSet.size)
      .map(f => f._1.setTotalFriendsOut(f._2))

    solSet = opResult ++ solSet.subtract(opResult)

    //totalfriends_in
    opResult = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(reverseEdgesRDD)
      .map(x => (x._2._1, x._2._2)).groupByKey()
      .mapValues(_.toSet.size)
      .map(f => f._1.setTotalFriendsIn(f._2))

    solSet = opResult ++ solSet.subtract(opResult)

    /*
     * Same as total friends but ommit ((FL,DestinatinID), 1) then reduce by key (FL,Des)
     * Then change the key to FL only and count how many destinationIDs appeared more than 
     * once in each group (FL) , these are the common friends
     */
    opResult = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(edgesRDD)
      .map(f => (f._2, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1._1, x._2)).groupByKey()
      .mapValues(x => x.filter(_ > 1).size)
      .map(f => f._1.setCommonFriendsOut(f._2))

    solSet = opResult ++ solSet.subtract(opResult)

    //common_in
    opResult = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(reverseEdgesRDD)
      .map(f => (f._2, 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1._1, x._2)).groupByKey()
      .mapValues(x => x.filter(_ > 1).size)
      .map(f => f._1.setCommonFriendsIn(f._2))

    solSet = opResult ++ solSet.subtract(opResult)

    /*
    * calculate FriendsMeasure
    * The number of connections between v and u friends.
    * The number of edges between the totalfriendsList and itself
    * 
    * Some of the 1st degree neigbours might not have outgoing edges and that will lead 
    * to losing the inital featurelist 
    * Add the diff between the sets and the result to construct the updated SolSet
    */
    opResult = solSet.flatMap { f => Seq((f.v, f), (f.u, f)) }
      .join(edgesRDD)
      .map(_._2.swap)
      .join(edgesRDD)
      .map(x => (x._2._1, (x._2._2, x._1)))
      .groupByKey()
      .mapValues { x =>
        val firstNB = x.map(_._1).toSet
        val secondNB = x.map(_._2).toSet
        secondNB.count(x => firstNB.contains(x))
      }.map(f => f._1.setFriendsMeasure(f._2))

    solSet = opResult ++ solSet.subtract(opResult)

    //v bidegree
    opResult = solSet.map(f => (f.v, f))
      .join(edgesRDD)
      .map(x => (x._2._2, (x._1, x._2._1)))
      .join(edgesRDD)
      .map(x => (x._2._1, x._2._2)).groupByKey()
      .map(x => x._1._2.setVbidegree(x._2.count(p => p == x._1._1)))

    solSet = opResult ++ solSet.subtract(opResult)

    //u bidegree
    opResult = solSet.map(f => (f.u, f))
      .join(edgesRDD)
      .map(x => (x._2._2, (x._1, x._2._1)))
      .join(edgesRDD)
      .map(x => (x._2._1, x._2._2)).groupByKey()
      .map(x => x._1._2.setUbidegree(x._2.count(p => p == x._1._1)))

    solSet = opResult ++ solSet.subtract(opResult)

    // RDD.saveAsTextFile fails, so collect the RDD and print on local machine
    // print to csv file

//    val outpath = new File("C:/tmp/spark/out/features.txt")
    val outpath = new File("C:/tmp/spark/out/features-test.txt")
    if (outpath.exists()) {
      outpath.delete()
    }

    val solSetCSV = solSet.map { _.toCsv }.collect()

    val pw = new PrintWriter(outpath)
    // pw.println(FeatureList.HeaderCsv)
    solSetCSV.foreach { pw.println(_) }
    pw.close

    println("----------- FINISHED -------------")
  }

}
