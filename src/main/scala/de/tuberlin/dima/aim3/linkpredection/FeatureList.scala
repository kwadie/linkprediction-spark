package de.tuberlin.dima.aim3.linkpredection

import org.apache.commons.lang3.builder.ReflectionToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import org.apache.commons.lang3.ObjectUtils
import org.apache.spark.util.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

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

  def toFeaturesArray = Array(v, u, v_degree, u_degree, commonFriends_Out, totalFriends_out, getPrefAttachment)

  def toCsv = toFeaturesArray.mkString(",") + "," + label

  def toVector = Vectors.dense(toFeaturesArray.map(_.toDouble))
  
  def toLabeledPoint = LabeledPoint(numericalLabel, toVector)
  
  def getPrefAttachment: Int = v_degree * u_degree

  def numericalLabel = if (label == "Y") 1.0 else 0.0
  
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

object FeatureList {
    def fromCsv(csv: String): FeatureList = {
    val split = csv.split(",")
    val it = split.iterator
  
    val u = it.next.toLong
    val v = it.next.toLong
    val v_degree = it.next.toInt
    val u_degree = it.next.toInt
    val commonFriends_Out = it.next.toInt
    val totalFriends_out = it.next.toInt
    val prefAttachment = it.next.toInt
    val label = it.next

    new FeatureList(u, v, label)
    				.setUdegree(u_degree)
    				.setVdegree(v_degree)
    				.setCommonFriendsOut(commonFriends_Out)
    				.setTotalFriendsOut(totalFriends_out)
  } 
}