#!/bin/sh
exec scala "$0" "$@"

!#

import java.io.{ File, BufferedWriter, FileWriter }
import scala.io.Source

class MonitorData(
  val initCFP: Double,
  val canceledCFP: Double,
  val unsuccessfulCFP: Double,
  val successfulCFP: Double,
  val ratio: Double,
  val averageK: Option[Double],
  val reducingTime: Double
) {

  def averageWith(ms: List[MonitorData]): MonitorData = {
    val n = ms.length

    def avg(neutral: Double, f: MonitorData => Double): Double =
      (ms.foldLeft(neutral) {
        case (acc, m) => acc + f(m)
      }) / (n + 1)

    new MonitorData(
      avg(this.initCFP,             { case m => m.initCFP }),
      avg(this.canceledCFP,         { case m => m.canceledCFP }),
      avg(this.unsuccessfulCFP,     { case m => m.unsuccessfulCFP }),
      avg(this.successfulCFP,       { case m => m.successfulCFP }),
      avg(this.ratio,               { case m => m.ratio }),
      if (this.averageK.isDefined) {
        Some(avg(this.averageK.get, { case m => m.averageK.get }))
      } else {
        None
      },
      avg(this.reducingTime,        { case m => m.reducingTime })
    )
  }

  override def toString: String =
    this.initCFP + ";" +
    this.canceledCFP + ";" +
    this.unsuccessfulCFP + ";" +
    this.successfulCFP + ";" +
    this.ratio + ";" +
    this.averageK + ";" +
    this.reducingTime

  def write(filePath: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(new File(filePath)))

    try {
      bw write (this.toString)
    } finally {
      bw.close
    }
  }

}

object DataExp extends App {

  val somePattern = """Some\(([\d\.]+)\)""".r

  def getLine(fileName: String): String =
    (Source fromFile fileName).getLines.toList.head

  val folder = new File(args(0))
  val folderFilter = args(1)
  val out = args(2)

  def lineToMonitorData(line: String): MonitorData = {
    val fields = line split ";"
    val avgK = fields(5) match {
      case somePattern(x) => Some(x.toDouble)
      case _              => None
    }

    new MonitorData(
      fields(0).toDouble,
      fields(1).toDouble,
      fields(2).toDouble,
      fields(3).toDouble,
      fields(4).toDouble,
      avgK,
      fields(6).toDouble
    )
  }

  val foldersToExplore =
    folder.list filter { name => name contains folderFilter }
  val monitorsData = (foldersToExplore map {
    foldername => lineToMonitorData(getLine(folder + "/" + foldername + "/monitor.csv"))
  }).toList
  val avgData =
    if (monitorsData.isEmpty) {
      new MonitorData(0, 0, 0, 0, 0, None, 0)
    } else {
      monitorsData.head.averageWith(monitorsData.tail)
    }

  avgData.write("./" + out)

}

DataExp.main(args)
