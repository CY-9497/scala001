package com.ch.similarityAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./words")
    val words = lines.flatMap(line => {
      line.split(" ")
    })
    val pairWords = words.map(word => {
      new Tuple2(word, 1)
    })
    val result = pairWords.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })
    result.foreach(println)
    sc.stop()
  }


}
