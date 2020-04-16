package io.ipolyzos.analysis

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnalysisFunctions {
  def balanceDFWithKMeans(df: DataFrame, reductionCount: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    new KMeans()
      .setK(reductionCount)
      .setMaxIter(30)
      .fit(df)
      .clusterCenters
      .toList
      .map(cc => (cc, 0))
      .toDF("features", "is_fraud")
  }

  def initRandomForestClassifier(df: DataFrame): RandomForestClassificationModel = {
    new RandomForestClassifier()
      .setLabelCol("is_fraud")
      .setFeaturesCol("features")
      .setMaxBins(700)
      .fit(df)
  }

  def calculateDistance(lat1: Double, long1: Double, lat2: Double, long2: Double): Double = {
    val r : Int = 6371 //Earth radius
    val latDistance : Double = Math.toRadians(lat2 - lat1)
    val lonDistance : Double = Math.toRadians(long2 - long1)
    val a : Double = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c : Double = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance : Double = r * c
    distance
  }
}
