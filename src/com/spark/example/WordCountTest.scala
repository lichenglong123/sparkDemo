package com.spark.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCountTest {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local");
    // 创建SparkContext
    val sc = new SparkContext(conf);
    // 输入文件
    val input = "F:\\testData\\test.txt";
    // 计算频次
    val count = sc.textFile(input).flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y);
    // 打印结果
    count.foreach(x => println(x._1 + ":" + x._2));
    // 结束
    sc.stop()
  }
}
