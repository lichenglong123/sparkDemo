package com.spark.example.sparkSQl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Spark 2.0 版本
  */
object SparkSessionJsonResource {

  def main(args: Array[String]): Unit = {
    val config=new SparkConf().setAppName("json").setMaster("local[2]");
    val sparkSeesion=new SparkSession.Builder().config(config).getOrCreate();
    val dataFrame= sparkSeesion.read.json("F:\\testData\\people.json");
     dataFrame.show();
    dataFrame.printSchema();
    dataFrame.select("age","name").show()
   val newage= dataFrame.col("age").plus(10).as("newAge")

   //dataFrame.filter(dataFrame.col("age").>=(31)).show();

  //  dataFrame.groupBy("age").count.show

    dataFrame.createOrReplaceTempView("people")

    sparkSeesion.sql("select * from people where age >31").show()

    sparkSeesion.sqlContext.sql("select name from people where age >31").show()

    sparkSeesion.stop();
  }

}
