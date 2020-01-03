package com.spark.example.sparkSQl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLDemo {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var userData: Array[String] = Array("1 lay 23", "2 marry 24", "3 gary 25")
  var userRDD: RDD[Row] = _
  var sqlContext: SQLContext = _
  var df: DataFrame = _

  def init(): Unit = {
    conf = new SparkConf().setAppName("spark sql demo").setMaster("local")
    sc = new SparkContext(conf)
    // 创建sqlContext
    sqlContext = new SQLContext(sc)
    // 创建schema
    var structFields = Array(StructField("id", IntegerType), StructField("name", StringType), StructField("age", IntegerType))
    var schema = new StructType(structFields)
    // 创建RDD
    userRDD = sc.parallelize(userData).map{x => val lines = x.split(" ");Row(lines(0).toInt, lines(1), lines(2).toInt)}
    // 创建dataFrame
    df = sqlContext.createDataFrame(userRDD, schema)
  }


  def main(args: Array[String]): Unit = {
    init()
    // dataFrame方式查询：查询年龄大于23岁的用户的姓名
    df.select("name").where("age > 23").show()
    // 注册为t_user表
    df.createOrReplaceTempView("t_user")
    // SQL方式查询：年龄大于23岁的用户的姓名
    sqlContext.sql("SELECT name FROM t_user WHERE age > 23").show()
  }


}
