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

    var sum:Long = 0
    var ben:Long = 0
    var wai:Long = 0

    for (file <- files) { //把每一个文件都遍历赋给file
      val name = file.getName //获取文件名
      val data = sc.textFile("D:\\BaiduNetdiskDownload\\数据\\数据\\"+name) //建立文件操作对象

      // rdd返回格式：(辖区编码，(车牌号，车辆注册地))
      // 其中辖区按辖区编码划分不同地区，车牌号标识车辆，车辆注册地标记本地/外地车辆
      val rdd = data.map(line => {
        val lines = line.split("\u0001")
        val jurisdiction = lines(55)//辖区编码
        val licensePlate = lines(51)//车牌号码
        val address = lines(52)//车辆注册地
//        println(jurisdiction)
//        println(licensePlate)
        (jurisdiction,(licensePlate,address))
      })
      rdd.count()
//      rdd.foreach(println)

      // 每出现一条数据代表车辆通行一次，为其赋1
      val rdd1 = rdd.map(x => {
        Tuple2(x, 1)
      }).reduceByKey((v1: Int, v2: Int) => {
        v1 + v2
      })

      // 规定：低于10条数据代表偶尔通行车辆，过滤出所需车辆数据
      val qualified = rdd1.filter(_._2 <= 10)
//      qualified.foreach(println)
//      所有车辆
      sum += qualified.count()

      // 筛选出本地车
      val rdd2 = qualified.filter(_._1._2._2 == "鄂")
      ben += rdd2.count()
//      rdd2.foreach(println)

      wai = sum - ben


    }
    println("车辆总数：" + sum)
    println("本地车辆数：" + ben)
    println("外地车辆数：" + wai)
    sc.stop()
  }

}
