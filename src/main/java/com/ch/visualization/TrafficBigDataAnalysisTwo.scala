package com.ch.visualization

import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import scala.collection.mutable.ArrayBuffer
/**
 * @author 渔郎
 * @Date 2022/6/11 9:46
 */
object TrafficBigDataAnalysisTwo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("TrafficBigDataAnalysisTwo")
    val sc = new SparkContext(conf)
    val dirFile = new File("D:\\BaiduNetdiskDownload\\数据\\数据") //定义一个目标文件夹对象
    val files = dirFile.listFiles //定义一个数组，文件夹中的每个文件作为数组的元素
    var resultRdd = sc.parallelize(List((("", ("", "", "")), 1)))


    for (file <- files) { //把每一个文件都遍历赋给file
      val name = file.getName //获取文件名
      val data = sc.textFile("D:\\BaiduNetdiskDownload\\数据\\数据\\"+name) //建立文件操作对象

      // rdd返回格式：(辖区编码，(车牌号，车辆注册地))
      // 其中辖区按辖区编码划分不同地区，车牌号标识车辆，车辆注册地标记本地/外地车辆
      val rdd = data.map(line => {
        val lines = line.split("\u0001")
        val id = lines(1)//设备id
        val year = lines(46)//年
        val month = lines(47)//月
        val day = lines(48)//日

        ((id,(year,month,day)),1)
      })
//      rdd.foreach(println)

      // 统计设备一天的过车数量
      val rdd1 = rdd.reduceByKey((v1: Int, v2: Int) => {
        v1 + v2
      })
//      rdd1.foreach(println)
      // 将rdd合并，便于循环后所有数据统一
      resultRdd = resultRdd.union(rdd1)


    }

    // 统计所有设备一天内记录的的过车数量
    val result = resultRdd.reduceByKey((v1: Int, v2: Int) => {
      v1 + v2
    })
//    result.foreach(println)

    // (设备id,(日过车数,1))
    val rdd2 = result.map(x=>{
      (x._1._1,(x._2,1))
    })
    // 求和
    val rdd3 = rdd2.reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    // 求设备的日过车平均数
    val rdd4 = rdd3.map(x => {
      (x._1, (x._2._1 / x._2._2))
    })

    rdd4.foreach(println)

    // 创建空数组
    val list = new ArrayBuffer[((String,(String,String,String)),Int)]()

    // rdd转化为list，便于循环嵌套，因为RDD之间无法循环嵌套，会报错
    val list1 = result.collect().toList
    val list2 = rdd4.collect().toList

    // 循环嵌套
    for (i <- 1 to list1.length){
      for (j <- 1 to list2.length){
        if (list1(i-1)._1._1 == list2(j-1)._1 && list1(i-1)._2 < list2(j-1)._2 / 2){
          list.append(list1(i-1))
        }
      }
    }

//    错误循环方式
//    result.foreach(x=>{
//      rdd4.foreach(y=>{
//        // 找出设备id相同，但日过车数低于日过车平均值的一半的设备
//        if (x._1._1 == y._1 && x._2 < y._2 / 2){
//          // 将其添加到数组中
//          list.append(x)
//        }
//      })
//    })
    // 打印结果
    list.foreach(println)

    // 保存异常设备的id异常时间和当日记录数
    sc.parallelize(list).saveAsTextFile("./result/TrafficBigDataAnalysisTwo")

    sc.stop()
  }

}
