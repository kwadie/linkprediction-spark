package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

/**
 * Hello world!
 *
 */
object App {
  
  def main(args: Array[String]) {
    
    //setting the environment
    val logFile = "D:\\tmp\\scala\\sampletext.txt" 
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    
    //building a dummy graph from nodes and edges data
    val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "advisor"), Edge(5L, 7L, "pi")))
                       
     val defaultUser = ("John Doe", "Missing")
     
     val graph:Graph[(String, String), String]  = Graph(users, relationships, defaultUser)
     
     //count all users who are post doc
     val postdocs = graph.vertices.filter {case(id,(name,pos)) => pos=="prof"}.count
     val piEdges = graph.edges.filter( edg => edg.attr=="advisor" ).count
     
     
     
     println(piEdges)
                       
                       
    
    
  }
  

}
