package com.spark.example.operator

import org.apache.spark.{SparkConf, SparkContext}

object MapTest {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("mapDemo").setMaster("local");
    val sc = new SparkContext(config);
    val list = List("java", "C++", "spark", "hadoop");
    val input = "F:\\testData\\test.txt";
    val count = sc.textFile(input)
    println("================file==================")
    count.collect().foreach(str => println(str));
    println("================flatMap==================")
    val flatMap = count.flatMap(x => x.split(" "));
    flatMap.collect().foreach(str => println(str));

    println("=================map=================")

    println("=================map=================")
    val mapRdd = flatMap.map(x => (x, 1))
    mapRdd.collect().foreach(str => println(str));




    println("==================================")
    val reduceByKeyRdd=mapRdd.reduceByKey((x,y)=>x+y);
    reduceByKeyRdd.foreach(str => println(str._1+":"+str._2));
    println("==================================")
    val groupByRdd=mapRdd.groupByKey()
    groupByRdd.collect().foreach(str => println(str));
    println("==================================")
    reduceByKeyRdd.saveAsTextFile("F:\\testData\\result.txt")










  }
}
