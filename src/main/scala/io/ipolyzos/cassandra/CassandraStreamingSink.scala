package io.ipolyzos.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

/**
 * A Cassandra streaming sink for writing spark structured streaming results
 * */
class CassandraStreamingSink(private val keyspace: String, private val table: String) {

  import org.apache.spark.sql.cassandra._

  private val sinkFormat = "org.apache.spark.sql.cassandra"
  private val options: Map[String, String] = Map(
    "keyspace" -> keyspace,
    "table" -> table
  )

  def writeStream(data: DataFrame, queryName: String, outputMode: OutputMode = OutputMode.Append()): StreamingQuery = {
    data.writeStream
      .queryName(queryName)
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write
          .cassandraFormat(table, keyspace)
          .mode(SaveMode.Append)
          .save()
      }
      .outputMode(outputMode)
      .start()
  }
}