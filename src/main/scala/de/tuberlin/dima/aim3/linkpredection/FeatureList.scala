package de.tuberlin.dima.aim3.linkpredection

@SerialVersionUID(114L)
class FeatureList (xv:Long, xu:Long) extends Serializable {
  
  var v:Long = xv
  var u:Long = xu
  var v_degree:Int = 0
  var u_degree:Int = 0
  var commonFriends:Int =0
  
  override def toString = 
  f"U: $u%s, V: $v%s, u_dg: $u_degree%s, v_dg: $v_degree%s, cf: $commonFriends%s"
  
  def setUdegree(ud:Int):FeatureList = {
    u_degree = ud
    return this
  }
  
  def setVdegree(vd:Int):FeatureList = {
    v_degree = vd
    return this
  }
  
  def setCommonFriends(cf:Int):FeatureList = {
    commonFriends = cf
    return this
  }
  
  
    
    
  

}