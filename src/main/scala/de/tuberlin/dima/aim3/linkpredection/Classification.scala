package de.tuberlin.dima.aim3.linkpredection

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.ml.tuning.CrossValidator

object Classification {

  val SEED = 0x1312
  val numIterations = 100

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val featuresPath = "C:/tmp/spark/out/features.txt"
    val csvData = sc.textFile(featuresPath, 4)
    val features = csvData.map(FeatureList.fromCsv)
      .map(_.toLabeledPoint)

    val splits = features.randomSplit(Array(0.6, 0.2, 0.2), seed = SEED)

    val training = splits(0).cache()
    val test = splits(1)

//        val model = SVMWithSGD.train(training, numIterations)
//        val model = NaiveBayes.train(training)
//        val model = LogisticRegressionWithSGD.train(training, numIterations)

//        val numClasses = 2
//    	val categoricalFeaturesInfo = Map[Int, Int]()
//    	val numTrees = 30
//    	val featureSubsetStrategy = "auto" // Let the algorithm choose.
//    	val impurity = "gini"
//    	val maxDepth = 4
//    	val maxBins = 32
//        
//        val model = RandomForest.trainClassifier(training, 2, categoricalFeaturesInfo, numTrees, 
//        							featureSubsetStrategy, impurity, maxDepth, maxBins, SEED)
//
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 10
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(training, boostingStrategy)

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println(f"AUC: $auROC")

    // save ROC curve to file for visualization
    // metrics.roc.toArray
    
    val featuresTestPath = "C:/tmp/spark/out/features.txt"
    val featuresTest = sc.textFile(featuresPath, 4)
      .map(FeatureList.fromCsv)
      .map(_.toLabeledPoint)

    val scoreAndLabelsTest = featuresTest.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metricsTest = new BinaryClassificationMetrics(scoreAndLabels)
    val auROCTest = metricsTest.areaUnderROC()

    println(f"AUC Test: $auROCTest")
  }

}