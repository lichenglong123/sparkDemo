package com.spark.example.sparkSQl

import org.apache.spark.sql

object SparkSessionTextResource {
  def main(args: Array[String]): Unit = {
    val sparkSession=new sql.SparkSession.Builder()
      .appName("text")
      .master("local")
      .getOrCreate();

  val dataFrame=sparkSession.read.text("F:\\testData\\test.txt")
    dataFrame.show();
    sparkSession.stop();

  }

}
