package de.tuberlin.dima.aim3.linkpredection

import org.apache.commons.lang3.builder.ReflectionToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import org.apache.commons.lang3.ObjectUtils

@SerialVersionUID(114L)
class FeatureList(xu: Long, xv: Long, xlabel: String) extends Serializable with Comparable[FeatureList] {

  var u: Long = xu
  var v: Long = xv
  
  var u_degree: Int = 0
  var v_degree: Int = 0
  
  var u_in: Int =0
  var v_in:Int =0
  
  var u_out:Int = 0
  var v_out: Int = 0
  
  var u_bi: Int =0
  var v_bi: Int =0
  
  var commonFriends_In: Int = 0
  var commonFriends_Out: Int = 0
  
  var totalFriends_in: Int = 0
  var totalFriends_out: Int = 0
  
  var friendsMeasure:Int =0;
  
  var label: String = xlabel
  
  override def toString = ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)
  
  def  headerCsv = "u,v,u_dg,u_in,u_out,u_bi,u_indensity,u_outdensity,u_bidensity,"+
                  "v_dg,v_in,v_out,v_bi,v_indensity,v_outdensity,v_bidensity,"+
                  "cf_in,cf_out,tf_in,tf_out,fm,pa,jcin,jcout,label"
  

  def asCsv = 
              f"$u,$v,$u_degree,$u_in,$u_out,$u_bi,$calcUinDensity,$calcUoutDensity,$calcUbiDensity,"+
              f"$v_degree,$v_in,$v_out,$v_bi,$calcVinDensity,$calcVoutDensity,$calcVbiDensity,"+
              f"$commonFriends_In,$commonFriends_Out,$totalFriends_in,$totalFriends_out,$friendsMeasure,"+
              f"$calcPrefAttachment,$calcJaccardIn,$calcJaccardOut,$label"

  //calculated features
  def calcPrefAttachment: Int = v_degree * u_degree
  def calcJaccardOut:Double =  if ( totalFriends_out != 0) commonFriends_Out.toDouble/totalFriends_out.toDouble else 0.0D 
  def calcJaccardIn:Double =  if ( totalFriends_in != 0) commonFriends_In.toDouble/totalFriends_in.toDouble else 0.0D 
  def calcVinDensity:Double =   if(v_degree != 0)   v_in.toDouble / v_degree.toDouble  else 0.0D
  def calcVoutDensity:Double =   if(v_degree != 0)   v_out.toDouble  / v_degree.toDouble  else 0.0D
  def calcVbiDensity:Double =   if(v_degree != 0)   v_bi.toDouble  / v_degree.toDouble  else 0.0D
  def calcUinDensity:Double =   if(u_degree != 0)   u_in.toDouble  / u_degree.toDouble  else 0.0D
  def calcUoutDensity:Double =   if(u_degree != 0)   u_out.toDouble  / u_degree.toDouble  else 0.0D
  def calcUbiDensity:Double =   if(u_degree != 0)   u_bi.toDouble  / u_degree.toDouble  else 0.0D

  def setUdegree(ud: Int): FeatureList = {
    u_degree = ud
    return this
  }
  
     def setUindegree(id: Int): FeatureList = {
    u_in = id
    return this
  }
   
    def setUoutdegree(id: Int): FeatureList = {
    u_out = id
    return this
  }

  def setVdegree(vd: Int): FeatureList = {
    v_degree = vd
    return this
  }
  
   def setVindegree(id: Int): FeatureList = {
    v_in = id
    return this
  }
   
    def setVoutdegree(id: Int): FeatureList = {
    v_out = id
    return this
  }
    
  def setVbidegree(d: Int): FeatureList = {
    v_bi = d
    return this
  }
  
    def setUbidegree(d: Int): FeatureList = {
    u_bi = d
    return this
  }
    
    
    
  def setCommonFriendsOut(cf: Int): FeatureList = {
    commonFriends_Out = cf
    return this
  }
  
    def setCommonFriendsIn(cf: Int): FeatureList = {
    commonFriends_In = cf
    return this
  }

  def setTotalFriendsOut(tf: Int): FeatureList = {
    totalFriends_out = tf
    return this
  }
  
  def setTotalFriendsIn(tf: Int): FeatureList = {
    totalFriends_in = tf
    return this
  }
  
   def setFriendsMeasure(fm: Int): FeatureList = {
    friendsMeasure = fm
    return this
  }

  override def equals(obj: Any) = {
    obj.isInstanceOf[FeatureList] && compareTo(obj.asInstanceOf[FeatureList]) == 0 
  }

  override def compareTo(other: FeatureList) = {
    if (v == other.v) {
      u.compareTo(other.u)
    } else {
      v.compareTo(other.v)
    }
  }

  override def hashCode() = Seq(u, v).hashCode

}


class VertexDegrees(d:Int, in:Int, out:Int){
  
  var degree:Int = d
  var indegree:Int = in
  var outdegree:Int = out
  
  override def toString = ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)
  
}