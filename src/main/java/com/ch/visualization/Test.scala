package com.ch.visualization

import breeze.linalg.{DenseVector, linspace, DenseMatrix => BDM}
import breeze.plot._

import scala.language.postfixOps
/**
 * @author 渔郎
 * @Date 2022/5/30 14:49
 */

object Test {
  def main(args: Array[String]): Unit = {

    val f = Figure()
    val p = f.subplot(0)
    val x = new DenseVector[Double](Array(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0))
    val y = new DenseVector[Double](Array(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0))
    p += plot(x, y)
    p += plot(x, y, '.')
    p.xlabel = "x axis"
    p.ylabel = "y axis"
    f.saveas("./lines.png")

    val p2 = f.subplot(2, 1, 1)
    val g = breeze.stats.distributions.Gaussian(0, 1)
    p2 += hist(g.sample(100000), 1000)
    p2.title = "A normal distribution"
    f.saveas("./subplots.png")

    val f2 = Figure()
    f2.subplot(0) += image(BDM.rand(200, 200))
    f2.saveas("./image.png")
  }
}