package de.tuberlin.dima.aim3.linkpredection

import org.apache.commons.lang3.builder.ReflectionToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import org.apache.commons.lang3.ObjectUtils
import org.apache.spark.util.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

@SerialVersionUID(114L)
class FeatureList(xu: Long, xv: Long, xlabel: String) extends Serializable with Comparable[FeatureList] {

  var u: Long = xu
  var v: Long = xv

  var u_degree: Int = 0
  var v_degree: Int = 0

  var u_in: Int = 0
  var v_in: Int = 0

  var u_out: Int = 0
  var v_out: Int = 0

  var u_bi: Int = 0
  var v_bi: Int = 0

  var commonFriends_In: Int = 0
  var commonFriends_Out: Int = 0

  var totalFriends_in: Int = 0
  var totalFriends_out: Int = 0

  var friendsMeasure: Int = 0;

  var label: String = xlabel

  // calculated features
  def calcPrefAttachment: Int = v_degree * u_degree
  def calcJaccardOut: Double = if (totalFriends_out != 0) commonFriends_Out.toDouble / totalFriends_out.toDouble else 0.0D
  def calcJaccardIn: Double = if (totalFriends_in != 0) commonFriends_In.toDouble / totalFriends_in.toDouble else 0.0D
  def calcVinDensity: Double = if (v_degree != 0) v_in.toDouble / v_degree.toDouble else 0.0D
  def calcVoutDensity: Double = if (v_degree != 0) v_out.toDouble / v_degree.toDouble else 0.0D
  def calcVbiDensity: Double = if (v_degree != 0) v_bi.toDouble / v_degree.toDouble else 0.0D
  def calcUinDensity: Double = if (u_degree != 0) u_in.toDouble / u_degree.toDouble else 0.0D
  def calcUoutDensity: Double = if (u_degree != 0) u_out.toDouble / u_degree.toDouble else 0.0D
  def calcUbiDensity: Double = if (u_degree != 0) u_bi.toDouble / u_degree.toDouble else 0.0D

  def numericalLabel = if (label == "Y") 1.0 else 0.0

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

  def toFeaturesArray = Array(u, v, u_degree, v_degree, u_in, u_out, u_bi, v_in, v_out, v_bi, 
		commonFriends_In, commonFriends_Out, totalFriends_in, totalFriends_out, friendsMeasure, 
		calcUinDensity, calcUoutDensity, calcUbiDensity, calcVinDensity, calcVoutDensity, calcVbiDensity, 
		calcPrefAttachment, calcJaccardIn, calcJaccardOut)

  def toCsv = toFeaturesArray.mkString(",") + "," + label
  def toVector = Vectors.dense(toFeaturesArray)
  def toLabeledPoint = LabeledPoint(numericalLabel, toVector)


  override def toString = ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)

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

class VertexDegrees(d: Int, in: Int, out: Int) {

  var degree: Int = d
  var indegree: Int = in
  var outdegree: Int = out

  override def toString = ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE)
}

object FeatureList {
  def fromCsv(csv: String): FeatureList = {
    val split = csv.split(",")
    val it = split.iterator

    val u = it.next.toDouble.toLong
	val v = it.next.toDouble.toLong
	val u_degree = it.next.toDouble.toInt
	val v_degree = it.next.toDouble.toInt
	val u_in = it.next.toDouble.toInt
	val u_out = it.next.toDouble.toInt
	val u_bi = it.next.toDouble.toInt
	val v_in = it.next.toDouble.toInt
	val v_out = it.next.toDouble.toInt
	val v_bi = it.next.toDouble.toInt
	val commonFriends_In = it.next.toDouble.toInt
	val commonFriends_Out = it.next.toDouble.toInt
	val totalFriends_in = it.next.toDouble.toInt
	val totalFriends_out = it.next.toDouble.toInt
	val friendsMeasure = it.next.toDouble.toInt
	
	val label = split.last

    new FeatureList(u, v, label)
      .setUdegree(u_degree)
      .setVdegree(v_degree)
      .setUindegree(u_in)
      .setUoutdegree(u_out)
      .setUbidegree(u_bi)
      .setVindegree(v_in)
      .setVoutdegree(v_out)
      .setVbidegree(v_bi)
      .setCommonFriendsIn(commonFriends_In)
      .setCommonFriendsOut(commonFriends_Out)
      .setTotalFriendsIn(totalFriends_in)
      .setTotalFriendsOut(totalFriends_out)
      .setFriendsMeasure(friendsMeasure)
  }

  val HeaderCsv = "u,v,u_dg,v_dg,u_in,u_out,u_bi,v_in,v_out,v_bi,cf_in,cf_out,tf_in,tf_out,fm," +
		  "u_indensity,u_outdensity,u_bidensity,v_indensity,v_outdensity,v_indensity,pa," +
		  "jcin,jcout,label"

}

