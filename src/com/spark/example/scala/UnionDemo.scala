package com.spark.example.scala

import org.apache.spark.{SparkConf, SparkContext}

object UnionDemo {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    // 创建SparkContext
    val sc = new SparkContext(conf);
    val rddA= sc.parallelize(List("AA","BB","CC"))
    val rddB= sc.parallelize(List("DD","BB","GG"))
   // val result=rddA.union(rddB);
   // result.foreach(println)
   // print(result.collect().mkString)

    val result=rddA.cartesian(rddB)
    result.foreach(println)




  }

}
