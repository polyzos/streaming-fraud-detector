package io.ipolyzos.batch

import java.util.concurrent.{ExecutorService, Executors}

import io.ipolyzos.SparkJob
import io.ipolyzos.analysis.AnalysisFunctions
import io.ipolyzos.schemas.{CustomerSchema, TransactionSchema}
import io.ipolyzos.utils.BulkUploadUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType, TimestampType}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object CassandraBulkUpload extends SparkJob("Cassandra Bulk Upload") {

  Logger.getLogger("org.apache").setLevel(Level.WARN)

  import spark.implicits._
  import org.apache.spark.sql.functions._

  private val cassandraFormat = "org.apache.spark.sql.cassandra"

  private val keyspace = "creditcard"

  private val customer = "customer"
  private val fraudTransactionTable = "fraud_transaction"
  private val nonFraudTransactionTable = "non_fraud_transaction"

  private val customerDataSource = "src/main/resources/data/customer.csv"
  private val transactionsDataSource = "src/main/resources/data/transactions.csv"
  private val columnSelection = List(
    $"cc_num",
    $"trans_num",
    $"trans_time",
    $"category",
    $"merchant",
    $"amt",
    $"merch_lat",
    $"merch_long",
    $"distance",
    $"age",
    $"is_fraud"
  )

  private val executorService: ExecutorService = Executors.newFixedThreadPool(3)
  private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executorService)

  BulkUploadUtils.createKeyspace()
  BulkUploadUtils.createTables()
  BulkUploadUtils.truncateTables()

  val customerDF: DataFrame = spark.read
    .option("header", "true")
    .schema(CustomerSchema.customerSchema)
    .csv(customerDataSource)
    .withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))

  customerDF.show(truncate = false)
  val fraudTransactionSchema: StructType = TransactionSchema.transactionSchema
    .add("is_fraud", DoubleType, nullable = true)

  val transactionsDF: DataFrame =  spark.read
    .option("header", "true")
    .schema(fraudTransactionSchema)
    .csv(transactionsDataSource)
    .withColumn("trans_date", split($"trans_date", "T").getItem(0))
    .withColumn("trans_time", concat_ws(" ", $"trans_date", $"trans_time"))
    .withColumn("trans_time", to_timestamp($"trans_time", "YYYY-MM-dd HH:mm:ss").cast(TimestampType))

  val distanceUDF: UserDefinedFunction = udf(AnalysisFunctions.calculateDistance _)

  val processedDF: DataFrame = transactionsDF.join(
    broadcast(customerDF), Seq("cc_num")
  )
    .withColumn("distance", lit(round(distanceUDF($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
    .select(columnSelection:_*)

  processedDF.cache()

  val fraudulentDF: Dataset[Row]    = processedDF.filter($"is_fraud" === 1)
  val nonFraudulentDF: Dataset[Row] = processedDF.filter($"is_fraud" === 0)

  val customerFutureWrite = Future {
    logger.info("Writing records to customer table.")
    customerDF.write
      .format(cassandraFormat)
      .mode("append")
      .options(Map(
        "keyspace" -> keyspace,
        "table" -> customer
      ))
      .save()
  }


  val fraudulentFutureWrite = Future {
    logger.info("Writing records to fraud_transactions table.")
    fraudulentDF.write
      .format(cassandraFormat)
      .mode("append")
      .options(
        Map(
          "keyspace" -> keyspace,
          "table" -> fraudTransactionTable
        ))
      .save()
  }

  val nonFraudulentFutureWrite = Future {
    logger.info("Writing records to non_fraud_transactions table.")
    nonFraudulentDF.write
      .format(cassandraFormat)
      .mode("append")
      .options(
        Map(
          "keyspace" -> keyspace,
          "table" -> nonFraudTransactionTable
        ))
      .save()
  }

  import scala.concurrent.duration._
  Await.result(
    Future.sequence(
      Seq(customerFutureWrite, fraudulentFutureWrite, nonFraudulentFutureWrite)
    ), 1 minutes)

    logger.info("Finished Cassandra Bulk Tables Upload.")
    executorService.shutdown()
}
