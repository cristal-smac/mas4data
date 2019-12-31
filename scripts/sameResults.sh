#!/bin/sh
exec scala "$0" "$@"
!#
object Check extends App {
  import scala.io.Source

  def getLines(fileName: String): List[String] =
    (Source fromFile fileName).getLines.toList

  val data1 = getLines(args(0))
  val data2 = getLines(args(1))

  def buildResults(lines: List[String]): Map[String, String] =
    lines.foldLeft(Map[String, String]()) {
      case (map, line) =>
        val split = line split " "
        map + (split(0) -> split(1))
    }

  val results1 = buildResults(data1)
  val results2 = buildResults(data2)

  val ((baseTitle, toCompareTitle), (base, toCompare)) =
    if (results2.size > results1.size)
      ((args(1), args(0)), (results2, results1))
    else
      ((args(0), args(1)), (results1, results2))

  println("-----------")
  println(baseTitle + " | " + toCompareTitle)
  println("-----------")

  var ok = true

  for ((key, value) <- base) {
    if (toCompare contains key) {
      if (toCompare(key) != value) {
        if (ok) ok = false
        println(key + ": " + value + ", " + toCompare(key))
      }
    }
    else {
      if (ok) ok = false
      println("MISSING key: " + key)
    }
  }

  if (ok) println("\nSAME RESULTS !")

}
