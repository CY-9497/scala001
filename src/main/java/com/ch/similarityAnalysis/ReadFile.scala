package com.ch.similarityAnalysis

import scala.io.{BufferedSource, Source}
/**
 * @author 渔郎
 * @Date 2022/5/15 10:09
 */
object ReadFile {
  def main(args: Array[String]): Unit = {


    //1. 获取数据源对象.
    val source: BufferedSource = Source.fromFile("./A Tale of Two Cities - Charles Dickens.txt", "GBK")
    //2.通过getLines()方法, 逐行获取文件中的数据.
    val lines = source.getLines()
    //3. 将获取到的每一条数据都封装到列表中.
    val list = lines.toList

    list.foreach(println)
  }


}
