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
object SandBox {
  
  def main(args: Array[String]) {
    
    //setting the environment
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    
    //by default the join will be on the first element of the sequence
   var set1 = sc.parallelize(Seq( ("A", "B"),("X","Y"),("Z","W")))
    var set2 = sc.parallelize(Seq(("A", "C"),("A","B"),("X","C"),("X","B"),("Y","C")))
    
    //only matched records, use left joins to join this with sol set    
    val totalFriends = set1.flatMap(f=> Seq(  (f._1,f) , (f._2,f) ) ).join(set2).map( x=> (x._2._1, x._2._2 ) ).groupByKey().mapValues ( _.toSet.size )  
    totalFriends.collect().foreach(println(_) )
    
    val commonFriends = set1.flatMap(f=> Seq(  (f._1,f) , (f._2,f) ) ).join(set2).map(f =>  ((f._2),1) ).reduceByKey((x,y)=> x+y).map(f => (f._1._1, f._2 )).filter(x=> x._2>1 )
    commonFriends.collect().foreach(println(_) )
    
    
    /*
    
    //joins can be done on pair RDD
    var set3 = set1.map(f=> (new JoinElement(f._1,f._2), "11" )  )
    var set4 = set2.map(f=> (new JoinElement(f._1,f._2), "12" )  )
    
    //joins can be done on pair RDD
    var set5 = set1.map(f=> (new UniJoinElement(f._1), "100" )  )
    var set6 = set2.map(f=> (new UniJoinElement(f._1), "200" )  )
    

    //expected 4
    val y = set1.join(set2)
    println("y = "+ y.count())
    
    val x = set3.join(set4)
    println( "x = " + x.count())
    
   // set5.foreach( x=> println( x._1.v + " "+ x._2 ))
    
     val z = set5 join set6
    println( "z = " +z.collect().size)
    
    val a = new UniJoinElement("A")
    val b = new UniJoinElement("A")
    
    //override equals works for both == and equals
     println( b==a)
     println( b.equals(a))
     
     //override equals doesn't affect eq
     println( b.eq(a))
     * 
     */
    


    
  }
  

}

@SerialVersionUID(115L)
class JoinElement (xv:String, xu:String) extends Serializable {
  
  var v:String = xv
  var u:String = xu
  
  override def equals(obj:Any)=  obj.isInstanceOf[JoinElement] && obj.asInstanceOf[JoinElement].v == this.v && obj.asInstanceOf[JoinElement].u == this.u 

}

@SerialVersionUID(118L)
class UniJoinElement (xv:String) extends Serializable {
  
  var v:String = xv
  
  
  override def equals(obj:Any)=  obj.isInstanceOf[UniJoinElement] && obj.asInstanceOf[UniJoinElement].v == this.v
  
  
}
