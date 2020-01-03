package com.spark.example.scala

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsDemo {


  def mapDoubleFunc(a: Int): (Int, Int) = {
    (a, a * 2)
  }


  def doubleFunc(iter: Iterator[Int]): Iterator[(Int, Int)] = {
    var res = List[(Int, Int)]()
    while (iter.hasNext) {
      val cur = iter.next;
      res.::=(cur, cur * 2)
    }
    res.iterator
  }

    def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    // 创建SparkContext
    val sc = new SparkContext(conf);
    val rdd= sc.parallelize(1 to 9, 3)

    //val mapResult = rdd.map(mapDoubleFunc)
    val mapResult = rdd.mapPartitions(doubleFunc)

    println(mapResult.collect().mkString)


  }
}
