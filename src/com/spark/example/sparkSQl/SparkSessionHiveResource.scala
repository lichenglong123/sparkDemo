package com.spark.example.sparkSQl

import org.apache.spark.sql.SparkSession

/**
  * Spark 2.0 读取Hive表中的数据
  */
object SparkSessionHiveResource {

  def main(args: Array[String]): Unit = {
    val sparkSession = new SparkSession.Builder()
      .appName("hiveResource")
      .master("local[1]")
      // sparkSQL 连接 hive 时需要这句
      .enableHiveSupport()
      .getOrCreate()

    val hiveData = sparkSession.sql("select * from hive limit 2");
    hiveData.show();
    sparkSession.stop()
  }

}
