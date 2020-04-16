package io.ipolyzos.batch

import io.ipolyzos.SparkJob
import io.ipolyzos.analysis.AnalysisFunctions
import io.ipolyzos.pipelines.PreprocessingPipelines
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType

object FraudDetectorModel extends SparkJob("Fraud Detector Training"){
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  import spark.implicits._
  import org.apache.spark.sql.functions._

  private val cassandraFormat = "org.apache.spark.sql.cassandra"

  private val keyspace = "creditcard"

  private val fraudTransactionTable = "fraud_transaction"
  private val nonFraudTransactionTable = "non_fraud_transaction"

  val fraudTransactionsDF = spark.read
    .format(cassandraFormat)
    .options(
      Map(
        "keyspace" -> keyspace,
        "table" -> fraudTransactionTable,
        "pushdown" -> "true"
      )
    )
    .load()
    .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

  val nonFraudTransactionsDF = spark.read
    .format(cassandraFormat)
    .options(
      Map(
        "keyspace" -> keyspace,
        "table" -> nonFraudTransactionTable,
        "pushdown" -> "true"
      )
    )
    .load()
    .select("cc_num" , "category", "merchant", "distance", "amt", "age", "is_fraud")

  val transactionDF = nonFraudTransactionsDF.union(fraudTransactionsDF)
  transactionDF.cache()

  transactionDF.show(truncate = false)

  val columnNames = List("category", "merchant", "distance", "amt", "age")

  val pipelineStages = PreprocessingPipelines.createFeatureExtractionPipeline(transactionDF.schema, columnNames)
  val preprocessingTransformerModel = new Pipeline().setStages(pipelineStages).fit(transactionDF)
  val featuresDF = preprocessingTransformerModel.transform(transactionDF)

  featuresDF.show(truncate = false)

  val Array(trainData, testData) = featuresDF.randomSplit(Array(0.8, 0.2))

  val featureLabelDF = trainData.select("features", "is_fraud").cache()

  val nonFraudDF = featureLabelDF.filter($"is_fraud" === 0)

  val fraudDF = featureLabelDF.filter($"is_fraud" === 1)
  val fraudCount = fraudDF.count()

  println("fraudCount: " + fraudCount)
  println("Before Balancing, nonFraudCount: " + nonFraudDF.count())

  val balancedNonFraudDF = AnalysisFunctions.balanceDFWithKMeans(nonFraudDF, fraudCount.toInt)
  println("After Balancing, nonFraudCount: " + balancedNonFraudDF.count())

  val finalFeaturesDF = fraudDF.union(balancedNonFraudDF)

  val randomForestModel = AnalysisFunctions.initRandomForestClassifier(finalFeaturesDF)
  val predictionsDF = randomForestModel.transform(testData)

  println("Predictions DF:")
  predictionsDF.show(truncate = false)

  val predictionsAndLabels = predictionsDF.select(
    col("prediction"), col("is_fraud").cast(DoubleType)
  ).rdd.map {
    case Row(prediction: Double, label: Double) => (prediction, label)
  }.cache()


  val tp = predictionsAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 1 }.count().toFloat
  val fp = predictionsAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 1 }.count().toFloat
  val tn = predictionsAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 0 }.count().toFloat
  val fn = predictionsAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 0 }.count().toFloat


  printf(s"""|=================== Confusion matrix ==========================
             |#############| %-15s                     %-15s
             |-------------+-------------------------------------------------
             |Predicted = 1| %-15f                     %-15f
             |Predicted = 0| %-15f                     %-15f
             |===============================================================
         """.stripMargin, "Actual = 1", "Actual = 0", tp, fp, fn, tn)


  println()



  val metrics =new MulticlassMetrics(predictionsAndLabels)

  /*True Positive Rate: Out of all fraud transactions, how  much we predicted correctly. It should be high as possible.*/
  println("True Positive Rate: " + tp/(tp + fn))  // tp/(tp + fn)

  /*Out of all the genuine transactions(not fraud), how much we predicted wrong(predicted as fraud). It should be low as possible*/
  println("False Positive Rate: " + fp/(fp + tn))

  println("Precision: " +  tp/(tp + fp))

  /* Save Preprocessing  and Random Forest Model */
  randomForestModel.write.overwrite().save("src/main/resources/models/random_forest_model")
  preprocessingTransformerModel.write.overwrite().save("src/main/resources/models/preprocessing_model")

}
