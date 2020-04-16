package io.ipolyzos.utils


import org.apache.spark.sql.SparkSession

import scala.util.Try
import com.datastax.spark.connector.cql.CassandraConnector

object BulkUploadUtils {

  private lazy val fraudTableCreationCQL: String = """
                                |CREATE TABLE IF NOT EXISTS creditcard.fraud_transaction (
                                |    cc_num text,
                                |    trans_time timestamp,
                                |    trans_num text,
                                |    category text,
                                |    merchant text,
                                |    amt double,
                                |    merch_lat double,
                                |    merch_long double,
                                |    distance double,
                                |    age int,
                                |    is_fraud double,
                                |    PRIMARY KEY(cc_num, trans_time)
                                |  )WITH CLUSTERING ORDER BY (trans_time DESC);
                                |
                                |""".stripMargin

  private lazy val nonFraudTableCreationCQL = """
                                |CREATE TABLE IF NOT EXISTS creditcard.non_fraud_transaction (
                                |    cc_num text,
                                |    trans_time timestamp,
                                |    trans_num text,
                                |    category text,
                                |    merchant text,
                                |    amt double,
                                |    merch_lat double,
                                |    merch_long double,
                                |    distance double,
                                |    age int,
                                |    is_fraud double,
                                |    PRIMARY KEY(cc_num, trans_time))
                                |    WITH CLUSTERING ORDER BY (trans_time DESC);
                                |""".stripMargin


  private lazy val kafkaOffsetsTableCreationCQL = """
                                |CREATE TABLE IF NOT EXISTS creditcard.kafka_offset (
                                |    partition int,
                                |    offset bigint,
                                |    PRIMARY KEY(partition));
                                |""".stripMargin


  private lazy val customerTableCreationCQL = """
                                |CREATE TABLE IF NOT EXISTS creditcard.customer (
                                |cc_num text,
                                |first text,
                                |last text,
                                |gender text,
                                |street text,
                                |city text,
                                |state text,
                                |zip text,
                                |lat double,
                                |long double,
                                |job text,
                                |dob timestamp,
                                |age int,
                                |PRIMARY KEY(cc_num));
                                |""".stripMargin

  private lazy val tablesNames = List("fraud_transaction", "non_fraud_transaction", "kafka_offset", "customer")
  private lazy val tableCreationCQLCommands = List(
    fraudTableCreationCQL, nonFraudTableCreationCQL, kafkaOffsetsTableCreationCQL, customerTableCreationCQL
  )

  def truncateTables(tables: List[String] = tablesNames)(implicit spark: SparkSession): Unit = {
    tables.foreach { table =>
      val connector = CassandraConnector(spark.sparkContext.getConf)
      Try(
        connector.withSessionDo { session =>
          session.execute(s"TRUNCATE TABLE metis.$table")
        }
      )
    }
  }

  def createKeyspace(keyspace: String = "creditcard", replicationFactor: Int = 1)(implicit spark: SparkSession): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)
    Try(
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |CREATE KEYSPACE IF NOT EXISTS $keyspace
             |WITH replication = {'class':'SimpleStrategy', 'replication_factor' : $replicationFactor};
             |""".stripMargin)
      }
    )
  }

  def createTables(tableCreationCqls: List[String] = tableCreationCQLCommands)(implicit spark: SparkSession): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)
    tableCreationCqls foreach { tableCreationCql =>
      connector.withSessionDo(_.execute(tableCreationCql))
    }
  }
}
