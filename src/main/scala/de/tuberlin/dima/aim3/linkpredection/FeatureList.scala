package de.tuberlin.dima.aim3.linkpredection

import org.apache.commons.lang3.builder.ReflectionToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import org.apache.commons.lang3.ObjectUtils

@SerialVersionUID(114L)
class FeatureList(xv: Long, xu: Long, xlabel: String) extends Serializable with Comparable[FeatureList] {

  var v: Long = xv
  var u: Long = xu
  var v_degree: Int = 0
  var u_degree: Int = 0
  var commonFriends_Out: Int = 0
  var totalFriends_out: Int = 0
  var prefAttachment: Int = v_degree * u_degree
  var label: String = xlabel

  override def toString = ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)

  def asCsv = f"$v,$u,$v_degree,$u_degree,$commonFriends_Out,$totalFriends_out,$getPrefAttachment,$label"

  def getPrefAttachment: Int = v_degree * u_degree

  def setUdegree(ud: Int): FeatureList = {
    u_degree = ud
    return this
  }

  def setVdegree(vd: Int): FeatureList = {
    v_degree = vd
    return this
  }

  def setCommonFriendsOut(cf: Int): FeatureList = {
    commonFriends_Out = cf
    return this
  }

  def setTotalFriendsOut(tf: Int): FeatureList = {
    totalFriends_out = tf
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