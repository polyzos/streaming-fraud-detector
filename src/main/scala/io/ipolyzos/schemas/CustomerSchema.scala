package io.ipolyzos.schemas

import org.apache.spark.sql.types.{DoubleType, StringType, StructType, TimestampType}

object CustomerEnum extends Enumeration {
  val cc_num = "cc_num"
  val first = "first"
  val last = "last"
  val gender = "gender"
  val street = "street"
  val city = "city"
  val state = "state"
  val zip = "zip"
  val lat = "lat"
  val long = "long"
  val job = "job"
  val dob = "dob"
}

object CustomerSchema {

  val customerSchema: StructType = new StructType()
    .add(CustomerEnum.cc_num, StringType, nullable = true)
    .add(CustomerEnum.first,  StringType, nullable = true)
    .add(CustomerEnum.last,   StringType, nullable = true)
    .add(CustomerEnum.gender, StringType, nullable = true)
    .add(CustomerEnum.street, StringType, nullable = true)
    .add(CustomerEnum.city,   StringType, nullable = true)
    .add(CustomerEnum.state,  StringType, nullable = true)
    .add(CustomerEnum.zip,    StringType, nullable = true)
    .add(CustomerEnum.lat,    DoubleType, nullable = true)
    .add(CustomerEnum.long,   DoubleType, nullable = true)
    .add(CustomerEnum.job,    StringType, nullable = true)
    .add(CustomerEnum.dob,    TimestampType, nullable = true)
}
