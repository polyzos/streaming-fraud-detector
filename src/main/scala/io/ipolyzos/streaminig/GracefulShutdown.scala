package io.ipolyzos.streaminig

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object GracefulShutdown {
  private val logger = Logger.getLogger(getClass.getName)

  var stopFlag:Boolean = false

  def checkShutdownMarker(): Unit = {
    if (!stopFlag) {
      stopFlag =  new java.io.File("/tmp/shutdownmarker").exists()
    }

  }

  /* Handle Structured Streaming graceful shutdown. */
  def handleGracefulShutdown(checkIntervalMillis:Int,
                             streamingQueries: List[StreamingQuery])(implicit spark: SparkSession) {

    var isStopped = false

    while (! isStopped) {
      logger.info("Calling awaitTerminationOrTimeout")
      isStopped = spark.streams.awaitAnyTermination(checkIntervalMillis)
      if (isStopped)
        logger.info("Streaming context is stopped. Exiting application...")
      else
        logger.info("Streaming App is still running. Timeout...")
      checkShutdownMarker()
      if (!isStopped && stopFlag) {
        logger.info("Stopping SSC now")
        streamingQueries.foreach(query => {
          query.stop()
        })
        spark.stop
        logger.info("SSC is stopped!!!!!!!")
      }
    }
  }
}
