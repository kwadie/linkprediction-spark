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

object Classification {

  val SEED = 0x1312
  val numIterations = 100

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pw = new PrintWriter("log-classification.txt")

    val featuresPath = "C:/tmp/spark/out/features.txt"
    val csvData = sc.textFile(featuresPath, 4)
    val featuresRaw = csvData.map(FeatureList.fromCsv)

    val splits = featuresRaw.randomSplit(Array(0.6, 0.2, 0.2), seed = SEED)

    val training = splits(0)
    val test = splits(2)


    // SVM and LogReg
    val feauresIdxSvmLr = List(12, 11, 4, 7, 8, 9)
    val trainingSvmLr = training.map(_.toLabeledPointSubset(feauresIdxSvmLr)).cache()
    val svm = SVMWithSGD.train(trainingSvmLr, 30)
    val logreg = LogisticRegressionWithSGD.train(trainingSvmLr, 30)

    val testSvmLr = test.map(_.toLabeledPointSubset(feauresIdxSvmLr))

    val scoredLabelsSvm = calcScores(testSvmLr, svm.predict)
    saveScores(scoredLabelsSvm, "svm-roc.txt")
    val aucSvm = calcAuc(scoredLabelsSvm)
    pw.println(f"SVM: $aucSvm")

    val scoredLabelsLr = calcScores(testSvmLr, logreg.predict)
    saveScores(scoredLabelsLr, "logreg-roc.txt")
    val aucLr = calcAuc(scoredLabelsLr)
    pw.println(f"LogReg: $aucLr")

    // Naive Bayes
    val feauresIdxNb = List(0, 12, 1, 6, 5, 7, 14)
    val trainingNb = training.map(_.toLabeledPointSubset(feauresIdxNb)).cache()
    val bayes = NaiveBayes.train(trainingNb)

    val testNb = test.map(_.toLabeledPointSubset(feauresIdxNb))
    val scoredLabelsNb = calcScores(testNb, bayes.predict)
    saveScores(scoredLabelsNb, "nb-roc.txt")
    val aucNb = calcAuc(scoredLabelsNb)
    pw.println(f"NB: $aucNb")

    // Tree Ensembles
    val trainingAll = featuresRaw.map(_.toLabeledPoint).cache()
    val rf = randomForest(trainingAll)
    val gbt = gradientBoostedTrees(trainingAll)

    val testAll = test.map(_.toLabeledPoint)

    val scoredLabelsRf = calcScores(testAll, rf.predict)
    saveScores(scoredLabelsRf, "rf-roc.txt")
    val aucRf = calcAuc(scoredLabelsRf)
    pw.println(f"RF: $aucRf")

    val scoredLabelsGbt = calcScores(testAll, gbt.predict)
    saveScores(scoredLabelsGbt, "gbt-roc.txt")
    val aucGbt = calcAuc(scoredLabelsGbt)
    pw.println(f"GBT: $aucGbt")

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