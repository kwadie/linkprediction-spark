package de.tuberlin.dima.aim3.linkpredection

import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

object ClassificationOnTest {

  val SEED = 0x1312
  val numIterations = 100

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pw = new PrintWriter("log-classification-2.txt")

    val featuresPath = "C:/tmp/spark/out/features.txt"
    val csvData = sc.textFile(featuresPath, 4)
    val training = csvData.map(FeatureList.fromCsv)

    val feauresIdxSvmLr = List(12, 11, 4, 7, 8, 9)
    val trainingSvmLr = training.map(_.toLabeledPointSubset(feauresIdxSvmLr)).cache()
    val logreg = LogisticRegressionWithSGD.train(trainingSvmLr, 30)

    val testPath = "C:/tmp/spark/out/features-test.txt"
    val test = sc.textFile(featuresPath, 4).map(FeatureList.fromCsv)

    val testSvmLr = test.map(_.toLabeledPointSubset(feauresIdxSvmLr))
    val scoredLabelsSvm = calcScores(testSvmLr, logreg.predict)
    val aucSvm = calcAuc(scoredLabelsSvm)
    pw.println(f"SVM: $aucSvm")
    
    pw.close()
  }

  def randomForest(data: RDD[LabeledPoint], numTrees: Int = 30, maxDepth: Int = 4, maxBins: Int = 32): RandomForestModel = {
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"

    return RandomForest.trainClassifier(data, 2, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins, SEED)
  }

  def gradientBoostedTrees(data: RDD[LabeledPoint], maxDepth: Int = 5, numIterations: Int = 10): GradientBoostedTreesModel = {
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = numIterations
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
    return GradientBoostedTrees.train(data, boostingStrategy)
  }

  def saveScores(scoredLabels: RDD[(Double, Double)], file: String) {
    val pw = new PrintWriter(file)
    val array = scoredLabels.toArray

    pw.println("score,label")
    array foreach {
      case (score, label) =>
        pw.println(f"$score,$label")
    }

    pw.close()
  }

  def calcScores(test: RDD[LabeledPoint], predict: Vector => Double) = {
    test.map(point => (predict(point.features), point.label))
  }

  def calcAuc(scoredLabels: RDD[(Double, Double)]) = {
    val metricsTest = new BinaryClassificationMetrics(scoredLabels)
    metricsTest.areaUnderROC()
  }

}