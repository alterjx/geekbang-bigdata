package com.geek.spark

import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {
  def main(args: Array[String]): Unit = {

    //"a": {(2,1)}
    //"banana": {(2,1)}
    //"is": {(0,2), (1,1), (2,1)}
    //"it": {(0,2), (1,1), (2,1)}
    //"what": {(0,1), (1,1)}

    val path = this.getClass.getClassLoader.getResource("files").getPath

    val conf = new SparkConf()
    conf.setAppName("spark_test")
    conf.setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
    val result = sc.wholeTextFiles(path)
      .map(pair => (pair._1.split("/").last, pair._2))
      .flatMap(pair => pair._2.split(" ").map(_ + "-" + pair._1))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(pair => {
        //  word 0
        val strings = pair._1.split("-")
        (strings(0), (strings(1), pair._2))
      })
      .groupByKey()
      .collectAsMap()
      .map(pair => (pair._1, pair._2.toList.sortBy(_._1)))
      .mkString(",")
    println(result)

  }
}
