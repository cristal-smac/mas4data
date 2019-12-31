#!/bin/sh
exec scala "$0" "$@"
!#
object Ratio extends App {
  import scala.io.Source

  def getLines(fileName: String): List[String] =
    (Source fromFile fileName).getLines.toList

  val allClassicData = getLines(args(0))
  val allAdaptiveData = getLines(args(1))

  def buildContributions(lines: List[String]): List[Int] =
    lines
    .map(line => (line split " ").tail map { _.toInt })
    .map(_.sum)

  val classicalContributions = buildContributions(allClassicData)
  val adaptiveContributions = buildContributions(allAdaptiveData)
  val maxC = classicalContributions.max.toDouble
  val maxA = adaptiveContributions.max.toDouble

  def buildRatio(max: Double, contributions: List[Int]): Double = {
    val min = contributions.min.toDouble

    min / max
  }

  println("Classical contribution : " + classicalContributions.sum)
  println("Adaptive contribution  : " + adaptiveContributions.sum)
  println("Classical fairness     : " + buildRatio(maxC, classicalContributions))
  println("Adaptive fairness      : " + buildRatio(maxA, adaptiveContributions))
  println("Adaptive better of     : " + (100 - ((maxA / maxC) * 100)) + " %")

}
Ratio.main(args)
