package com.ch.visualization

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * @author 渔郎
 * @Date 2022/6/9 17:28
 */
object TrafficBigDataAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("TrafficBigDataAnalysis")
    val sc = new SparkContext(conf)
    val dirFile = new File("D:\\BaiduNetdiskDownload\\数据\\数据") //定义一个目标文件夹对象
    val files = dirFile.listFiles //定义一个数组，文件夹中的每个文件作为数组的元素
    for (file <- files) { //把每一个文件都遍历赋给file
      val name = file.getName //获取文件名
      val data = sc.textFile("D:\\BaiduNetdiskDownload\\数据\\数据\\"+name) //建立文件操作对象
      val rdd = data.flatMap(line => {
        line.split("\u0001")
      })
      rdd.foreach(println)


    }
    sc.stop()
  }

}
