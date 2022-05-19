package com.ch.similarityAnalysis

import java.io.File
import scala.io.Source

/**
 * @author 渔郎
 * @Date 2022/5/17 10:46
 */
object ClimateAnalysis {
  def main(args: Array[String]): Unit = {
    val dirfile = new File("./ndp040") //定义一个目标文件夹对象
    val files = dirfile.listFiles //定义一个数组，文件夹中的每个文件作为数组的元素
    for (file <- files) { //把每一个文件都遍历赋给file
      val data = Source.fromFile(file) //建立文件操作对象
      var strs = data.getLines().map(x => {
        val id = x.substring(1, 7).trim()//截取有效信息，trim去除多余空格
        val year = x.substring(8, 13).trim()//截取有效信息，trim去除多余空格
        val month = x.substring(14, 17).trim()//截取有效信息，trim去除多余空格
        val day = x.substring(18, 21).trim()//截取有效信息，trim去除多余空格
        val msg = id + "观测点-"+year + "年" + month + "月" + day + "日"
        val minTemp = x.substring(23, 29).trim()//截取有效信息，trim去除多余空格
        val aveTemp = x.substring(31, 37).trim()//截取有效信息，trim去除多余空格
        val maxTemp = x.substring(39, 45).trim()//截取有效信息，trim去除多余空格
        ((id,year,month,day), aveTemp)
      })
      //以每年的1月1日为时间窗口，获取每年1月1日的平均气温并过滤错误数据
      strs = strs.filter(x => {
        x._1._2.toInt==1936&&x._1._3.toInt == 1&&x._1._4.toInt == 1&&x._2 != "999.9"
      })
      strs.foreach(println)


    }
//    files.toList
//    files.foreach(println)

  }
}