package com.troy

import org.apache.spark.sql.{DataFrame, SparkSession}

object TopK {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "D:/spark-warehouse"
    val session = SparkSession.builder()
      .appName("sparkSession")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import session.implicits._
    import session.sql
    //session.sql("use default")
    session.sql("show tables").cache().show()

//    val file = "/operation_data.txt.COMPLETED"
//    session.read.textFile(file).cache().foreach(print _)

  }
}