package com.spark.example.scala

import org.apache.spark.{SparkConf, SparkContext}

object HelloWord {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    // 创建SparkContext
    val sc = new SparkContext(conf);
    val rdd= sc.parallelize(List(List(List("a1","b2","c3","d4"),List("e9","f8","g7","h6")),List("dd","gg")))
    rdd.flatMap(x=>x).map(y=>y).foreach(println)
    rdd.mapPartitions(x=>x).map(y=>y).foreach(println)

  }

}
