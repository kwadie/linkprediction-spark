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
object FeaturesCalc {
  
  def main(args: Array[String]) {
    
    //setting the environment
    val inputPath = "D:\\data sets\\slashdot0902\\Slashdot0902.txt" 
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
   
    //spark Util to construct a graph from adjacency list 
    val graph = GraphLoader.edgeListFile(sc, inputPath)
    
    //get the in+out degrees of each vertex
    val degrees:VertexRDD[Int] = graph.degrees
    
   
    //FIXME: Sample easy and hard negative too
    //construct a sample of input data (positive links) for dev, Return an RDD[Edge[Int]
    //Then map each edge to a FeatureList object
    val pSample:RDD[FeatureList] = graph.edges.sample(false, 0.001)
                                   .map { edge => new FeatureList(edge.srcId,edge.dstId)}
    
    
    //create an indexed VertexRDD of (FeatureList) where vertexId = featureList.U for joins
    val vertexOnU:VertexRDD[FeatureList] = VertexRDD(pSample.map { fl => (fl.u,fl) })
    
    //join the degrees dataset with the vertexOnU on the u vertex and return the feature list including the u degree
    var solSet_Idx:VertexRDD[FeatureList] = degrees.innerJoin(vertexOnU)( (id,degree,fl) => fl.setUdegree(degree)  )
    
    //do the same for v but starting from last join result object to keep prev calcs in the featurelist object
    val vertexOnV:VertexRDD[FeatureList] = VertexRDD( solSet_Idx.map { js => ( js._2.v ,js._2) })
    solSet_Idx = degrees.innerJoin(vertexOnV)( (id,degree,fl) => fl.setVdegree(degree) )
    
    
    
    
    val solSet_RDD:RDD[FeatureList] = solSet_Idx.map{ solSetTuple =>
      
      
      val u = solSetTuple._2.u
      val v = solSetTuple._2.v
      var commonFriends:Int = 0
      
      try{
      //get all edges that contains either u or v
      val edgs:RDD[Edge[Int]] =  graph.edges.filter { edge => edge.srcId == u || edge.srcId == v || edge.dstId == u || edge.dstId == v}
      
      /*
       * 1.map each u||v edge into the other vertex value (ex: u-x ret x , x-v ret x..etc
       * 2.countByValue will return Map of vertexId and its repeats (ex: x=2, u=1..etc) 
       * 3.vertices appearing more than once are common friends
      
     
       commonFriends = edgs.map ( e => 
       if(e.srcId == u || e.srcId == v){ e.dstId }else{ e.srcId } 
       ).countByValue().count(p => p._2 > 1)
        */
      }catch{
        
        case e:Exception => println("OPPS "+ e.getMessage + " [sT] "+e.getStackTrace)
      }
       
      solSetTuple._2.setCommonFriends(commonFriends)


    }
    
 
   solSet_RDD.foreach(println(_))
    
   
    
    
    
    
    
    
   
    
    
    
    
    
   
                       
                       
    
    
  }
  

}
