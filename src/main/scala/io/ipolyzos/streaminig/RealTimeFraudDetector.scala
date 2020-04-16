package io.ipolyzos.streaminig

import io.ipolyzos.SparkJob
import io.ipolyzos.analysis.AnalysisFunctions
import io.ipolyzos.cassandra.CassandraStreamingSink
import io.ipolyzos.models.TransactionKafka
import io.ipolyzos.schemas.TransactionSchema
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

object RealTimeFraudDetector extends SparkJob("Structured Streaming Job to detect fraud transaction"){

  import spark.implicits._
  import org.apache.spark.sql.functions._

  private val cassandraFormat = "org.apache.spark.sql.cassandra"

  private val keyspace = "creditcard"
  private val customerTable = "customer"
  private val fraudTransactionTable = "fraud_transaction"
  private val nonFraudTransactionTable = "non_fraud_transaction"

  private val columnSelection = List(
    $"cc_num",
    $"trans_num",
    to_timestamp($"trans_time", "yyyy-MM-dd HH:mm:ss") as "trans_time",
    $"category",
    $"merchant",
    $"amt",
    $"merch_lat",
    $"merch_long",
    $"distance",
    $"age",
    $"partition",
    $"offset")

  val customerDF = spark.read
    .format(cassandraFormat)
    .options(
      Map(
        "keyspace" -> keyspace,
        "table" -> customerTable,
        "pushdown" -> "true"
      )
    )
    .load()
    .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))
  customerDF.cache()

  val kafkaInputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("enable.auto.commit", "false")
    .option("group.id", "RealTime Creditcard FraudDetection")
    .load()
    .withColumn("transaction", from_json($"value".cast(StringType), TransactionSchema.transactionSchema))
    .as[TransactionKafka]

  val transactionStream = kafkaInputStream
    .selectExpr("transaction.*", "partition", "offset")
    .withColumn("amt", lit($"amt").cast(DoubleType))
    .withColumn("merch_lat", lit($"merch_lat").cast(DoubleType))
    .withColumn("merch_long", lit($"merch_long").cast(DoubleType))
    .drop("first")
    .drop("last")

  val distanceUDF = udf(AnalysisFunctions.calculateDistance _)

  spark.sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = 52428800")

  val processedTransactionDF = transactionStream.join(broadcast(customerDF), Seq("cc_num"))
    .withColumn("distance", lit(round(distanceUDF($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
    .select(columnSelection:_*)

  val coloumnNames = List("cc_num", "category", "merchant", "distance", "amt", "age")

  val preprocessedModelPath = "src/main/resources/models/preprocessing_model"
  val randomForestModelPath = "src/main/resources/models/random_forest_model"

  val preprocessedModel = PipelineModel.load(preprocessedModelPath)
  val featureTransactionDF = preprocessedModel.transform(processedTransactionDF)

  val randomForestModel = RandomForestClassificationModel.load(randomForestModelPath)
  val predictionsDF = randomForestModel.transform(featureTransactionDF)
    .withColumnRenamed("prediction", "is_fraud")

  val fraudPredictionDF = predictionsDF.filter($"is_fraud" === 1.0)
  val nonFraudPredictionDF = predictionsDF.filter($"is_fraud" =!= 1.0)

  val fraudTransactionsCassandraSink = new CassandraStreamingSink(keyspace, fraudTransactionTable)
  val fraudQuery = fraudTransactionsCassandraSink.writeStream(fraudPredictionDF, "fraudQuery")

  val nonFraudTransactionsCassandraSink = new CassandraStreamingSink(keyspace, nonFraudTransactionTable)
  val nonFraudQuery = nonFraudTransactionsCassandraSink.writeStream(nonFraudPredictionDF, "nonFraudQuery")

  GracefulShutdown.handleGracefulShutdown(1000, List(fraudQuery, nonFraudQuery))
}
