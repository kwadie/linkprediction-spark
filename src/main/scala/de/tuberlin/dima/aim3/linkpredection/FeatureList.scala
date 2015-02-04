package de.tuberlin.dima.aim3.linkpredection

@SerialVersionUID(114L)
class FeatureList (xv:Long, xu:Long) extends Serializable {
  
  var v:Long = xv
  var u:Long = xu
  var v_degree:Int = 0
  var u_degree:Int = 0
  var commonFriends_Out:Int =0
  var totalFriends_out:Int =0
  var prefAttachment:Int = v_degree * u_degree
  
  override def toString = 
  f"V: $v%s, U: $u%s, v_dg: $v_degree%s, u_dg: $u_degree%s, cf_out: $commonFriends_Out%s, prefA: $getPrefAttachment%s, tf_out: $totalFriends_out%s "
  
  def getPrefAttachment:Int = v_degree * u_degree
  
  def setUdegree(ud:Int):FeatureList = {
    u_degree = ud
    return this
  }
  
  def setVdegree(vd:Int):FeatureList = {
    v_degree = vd
    return this
  }
  
  def setCommonFriendsOut(cf:Int): FeatureList = {
    commonFriends_Out = cf
    return this
  }
  
    def setTotalFriendsOut(tf:Int): FeatureList = {
    totalFriends_out = tf
    return this
  }
  
  
    
    
  

}