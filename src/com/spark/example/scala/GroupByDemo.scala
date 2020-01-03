package com.spark.example.scala

import org.apache.spark.{SparkConf, SparkContext}

object GroupByDemo {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    // 创建SparkContext
    val sc = new SparkContext(conf);
    val rddA= sc.parallelize(List("tomm","jack","lucy"),3)

    // val res= rddA.groupBy(_.length)
     // res.foreach(print)

  //  println(res.collect().mkString)

    val res=rddA.filter(x=> x.contains("tom"))
    res.foreach(println)




  }
}
