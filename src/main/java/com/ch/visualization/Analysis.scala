package com.ch.visualization

import breeze.linalg.{DenseVector, linspace, DenseMatrix => BDM}
import breeze.plot._
import org.apache.commons.collections.IteratorUtils
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @author 渔郎
 * @Date 2022/5/17 10:46
 */
object Analysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("Analysis")
    val sc = new SparkContext(conf)
    val dirfile = new File("./ndp040") //定义一个目标文件夹对象
    val files = dirfile.listFiles //定义一个数组，文件夹中的每个文件作为数组的元素

    //定义两个可变长数组
    val X = new ArrayBuffer[Double]()
    val Y = new ArrayBuffer[Double]()
    for (file <- files) { //把每一个文件都遍历赋给file
      val name = file.getName//获取文件名
      val data = sc.textFile("./ndp040/"+name) //建立文件操作对象
      var strs = data.map(x => {
        val id = x.substring(1, 7).trim() //截取有效信息，trim去除多余空格
        val year = x.substring(8, 13).trim() //截取有效信息，trim去除多余空格
        val month = x.substring(14, 17).trim() //截取有效信息，trim去除多余空格
        val day = x.substring(18, 21).trim() //截取有效信息，trim去除多余空格
        val msg = id + "观测点-" + year + "年" + month + "月" + day + "日"
        val minTemp = x.substring(23, 29).trim() //截取有效信息，trim去除多余空格
        val aveTemp = x.substring(31, 37).trim() //截取有效信息，trim去除多余空格
        val maxTemp = x.substring(39, 45).trim() //截取有效信息，trim去除多余空格
        ((id, year, month, day), aveTemp)
      })
      //以每年的1月1日为时间窗口，获取每年1月1日的平均气温并过滤错误数据
      strs = strs.filter(x => {
        x._1._2.toInt == 1936 && x._1._3.toInt == 1 && x._1._4.toInt == 1 && x._2 != "999.9"
      })
      //strs.foreach(println)


      val list = strs.collect().toList//化为数组
      for (l<- list){
        X+=(l._1._1.toDouble)
        Y+=(l._2.toDouble)
      }
      Y.foreach(println)


    }

    val f = Figure()
    val p = f.subplot(0)
    val x = new DenseVector[Double](X.toArray)
    val y = new DenseVector[Double](Y.toArray)
    p += plot(x, y)
    p += plot(x, y, '.')
    p.xlabel = "观测站ID"
    p.ylabel = "1936年1月1日各观测站温度"
    f.saveas("./分析结果-1936年1月1日各观测站温度.png")

    sc.stop()

  }
}
