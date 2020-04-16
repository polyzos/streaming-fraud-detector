package io.ipolyzos.models

import java.sql.Timestamp

case class Transaction(cc_num:String,
                       first:String,
                       last:String,
                       trans_num:String,
                       trans_time: Timestamp,
                       //unix_time:String,
                       category:String,
                       merchant:String,
                       amt:String,
                       merch_lat: String,
                       merch_long:String)

case class TransactionKafka(topic: String,
                            partition: Int,
                            offset: Long,
                            timestamp: Timestamp,
                            timestampType:Int,
                            transaction:Transaction)
