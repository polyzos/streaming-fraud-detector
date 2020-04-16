package io.ipolyzos.streaminig.kafka

import java.io.File
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.{Date, Properties, TimeZone}

import com.google.gson.{Gson, JsonObject}
import io.ipolyzos.schemas.TransactionEnum
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object TransactionsProducer extends App {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val topic = "transactions"

  val kafkaProducer = new KafkaProducer[String, String](props)

  val gson = new Gson()
  val inputPath = "src/main/resources/data/transactions.csv"

  val file = new File(inputPath)

  import collection.JavaConverters._

  CSVParser
    .parse(file, Charset.forName("UTF-8"), CSVFormat.DEFAULT)
    .iterator()
    .asScala
    .foreach { record =>
      val obj = new JsonObject

      val isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      isoFormat.setTimeZone(TimeZone.getTimeZone("IST"));
      val d = new Date()
      val timestamp = isoFormat.format(d)
      val unix_time = d.getTime

      obj.addProperty(TransactionEnum.cc_num, record.get(0))
      obj.addProperty(TransactionEnum.first, record.get(1))
      obj.addProperty(TransactionEnum.last, record.get(2))
      obj.addProperty(TransactionEnum.trans_num, record.get(3))
      obj.addProperty(TransactionEnum.trans_time, timestamp)
      obj.addProperty(TransactionEnum.category, record.get(7))
      obj.addProperty(TransactionEnum.merchant, record.get(8))
      obj.addProperty(TransactionEnum.amt, record.get(9))
      obj.addProperty(TransactionEnum.merch_lat, record.get(10))
      obj.addProperty(TransactionEnum.merch_long, record.get(11))
      val json: String = gson.toJson(obj)

      println("Transaction Record: " + json)
      val producerRecord = new ProducerRecord[String, String](topic, json)
      kafkaProducer.send(producerRecord, (recordMetadata: RecordMetadata, e: Exception) => {
        if (e != null) println("AsynchronousProducer failed with an exception" + e)
        else {
          println("Sent data to partition: " + recordMetadata.partition + " and offset: " + recordMetadata.offset)
        }
      })
      Thread.sleep(500)
    }
}
