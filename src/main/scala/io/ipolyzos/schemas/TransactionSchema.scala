package io.ipolyzos.schemas

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}


object TransactionEnum extends Enumeration {
  val cc_num = "cc_num"
  val first = "first"
  val last = "last"
  val trans_num = "trans_num"
  val trans_date = "trans_date"
  val trans_time = "trans_time"
  val unix_time = "unix_time"
  val category = "category"
  val merchant = "merchant"
  val amt = "amt"
  val merch_lat = "merch_lat"
  val merch_long = "merch_long"
  val distance = "distance"
  val age = "age"
  val is_fraud = "is_fraud"
  val kafka_partition = "partition"
  val kafka_offset = "offset"
}

object TransactionSchema {

  val transactionSchema: StructType = new StructType()
    .add(TransactionEnum.cc_num,    StringType, nullable = true)
    .add(TransactionEnum.first,     StringType, nullable = true)
    .add(TransactionEnum.last,      StringType, nullable = true)
    .add(TransactionEnum.trans_num, StringType, nullable = true)
    .add(TransactionEnum.trans_date,StringType, nullable = true)
    .add(TransactionEnum.trans_time,StringType, nullable = true)
    .add(TransactionEnum.unix_time, LongType,   nullable = true)
    .add(TransactionEnum.category,  StringType, nullable = true)
    .add(TransactionEnum.merchant,  StringType, nullable = true)
    .add(TransactionEnum.amt,       DoubleType, nullable = true)
    .add(TransactionEnum.merch_lat, DoubleType, nullable = true)
    .add(TransactionEnum.merch_long,DoubleType, nullable = true)
}
