package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import scala.util.control.Breaks._
import java.io.PrintWriter
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint

object SubsetSelection {

  val featuresPath = "C:/tmp/spark/out/features.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val csvData = sc.textFile(featuresPath, 4)
    val features = csvData.map(FeatureList.fromCsv)
    val splits = features.randomSplit(Array(0.6, 0.2, 0.2), seed = Classification.SEED)

    val training = splits(0)
    val validation = splits(1)

    val first = training.take(1)(0)
    var subset: List[Int] = Nil
    var currentAuc = 0.0

    val len = first.toFeaturesArray.length

    val pw = new PrintWriter("log.txt")

    var stop = false
    while (!stop) {
      val indexesToTry = 0 until len filterNot (subset.contains)
      var bestAuc = currentAuc
      var bestIdx = -1

      indexesToTry foreach { i =>
        val idx = subset :+ i
        val trainingSubset = training.map(_.toLabeledPointSubset(idx)).cache()

        // val model = LogisticRegressionWithSGD.train(trainingSubset, 20)
        // val model = NaiveBayes.train(trainingSubset)
        val model = SVMWithSGD.train(trainingSubset, 20)

        val validationSubset = validation.map(_.toLabeledPointSubset(idx))
        val auc = auROC(validationSubset, model.predict)

        if (auc > bestAuc) {
          bestAuc = auc
          bestIdx = i
        }

        pw.println(f"$idx: $auc")
        pw.flush()
      }

      if (Math.abs(currentAuc - bestAuc) < 1e-6 || bestIdx == -1) {
        stop = true
      } else {
        subset = subset :+ bestIdx
        currentAuc = bestAuc
      }
    }

    pw.println()
    pw.println(f"best validation auc: $currentAuc, with indexes $subset: features: ${FeatureList.names(subset)}")

    val trainingSubset = training.map(_.toLabeledPointSubset(subset)).cache()

    // val model = LogisticRegressionWithSGD.train(trainingSubset, 20)
    // val model = NaiveBayes.train(trainingSubset)
    val model = SVMWithSGD.train(trainingSubset, 20)

    val test = splits(2).map(_.toLabeledPointSubset(subset))
    val testAuc = auROC(test, model.predict)
    pw.println(f"test auc: $testAuc")
    pw.close()
  }

  def auROC(data: RDD[LabeledPoint], predict: Vector => Double) = {
    Classification.calcAuc(Classification.calcScores(data, predict))
  }

}