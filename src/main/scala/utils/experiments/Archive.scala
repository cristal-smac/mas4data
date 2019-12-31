package utils.experiments

import java.io.File

import utils.config.ConfigurationBuilder
import utils.files.FileWriter

import scala.collection.mutable
import scala.io.Source

/** Allow an agent to record an event with the corresponding timestamp. */
class Archive[T, U](
  name: String = "",
  initialHistorical: mutable.Queue[(Long, T)] = mutable.Queue[(Long, T)]()
) {

  // Configuration object
  private lazy val config = ConfigurationBuilder.config

  // Timed record
  type Record = (Long, T)

  // A timeline event is an aggregation of records of the archive
  type TimelineEvent = (Long, U)

  // Historical of the records
  private val historical: mutable.Queue[Record] = initialHistorical

  /** Secondary constructor. Build an archive from a previously wrote archive.
    *
    * @param name       name of the archive
    * @param filePath   path of the file which contains the previously wrote
    *                   archive
    * @param withHeader if <code>true</code> the file contains a header
    * @param toRecord   function which build a record from a string
    */
  def this(
    name: String,
    filePath: String,
    withHeader: Boolean,
    toRecord: String => (Long, T)
  ) = {
    this(
      name,
      if (withHeader) {
        Source.fromFile(filePath)
              .getLines
              .drop(1)
              .map(toRecord)
              .foldLeft(mutable.Queue[(Long, T)]()) {
          case (acc, record) =>
            acc.enqueue(record)
            acc
        }
      } else {
        Source.fromFile(filePath)
              .getLines
              .map(toRecord)
              .foldLeft(mutable.Queue[(Long, T)]()) {
          case (acc, record) =>
            acc.enqueue(record)
            acc
        }
      }
    )
  }

  /** Add a record to the archive.
    *
    * @param record record to add to the archive
    */
  def addRecord(record: T): Unit = {
    val timestamp = System.currentTimeMillis

    this.historical.enqueue((timestamp, record))
  }

  /** Fuse this archive with an other archive.
    *
    * @param archive        other archive to fuse this archive with
    * @param newArchiveName name of the fusion result archive
    * @return a new archive, fusion of this archive with another archive
    */
  def fuseWith(archive: Archive[T, U], newArchiveName: String): Archive[T, U] = {
    val newHistorical = (this.historical ++ archive.historical) sortWith {
      case ((t1, _), (t2, _)) => t1 < t2
    }

    new Archive[T, U](newArchiveName, newHistorical)
  }

  /** Write the historical of the archive in a CSV file.
    *
    * @param header    header of the CSV file
    * @param toCsvLine function which converts a record to a CSV line
    */
  def writeCSV(header: String, toCsvLine: T => String): Unit = {
    val historicalFileName: String =
      this.config.resultPath + "historical_" + this.name + ".csv"
    val file = new File(historicalFileName)
    val stringHistorical = this.historical.toIterator map {
      case (timestamp, record) => timestamp + ";" + toCsvLine(record) + "\n"
    }
    val completeHeader = "Timestamp;" + header + "\n"

    FileWriter.writeWith(file, Iterator(completeHeader) ++ stringHistorical)
  }

  // Create a timeline dividing the historical into steps
  protected def createTimeline(
    start: Long,
    step: Long,
    agg: mutable.Queue[Record] => TimelineEvent,
    replaceTimestamp: Boolean
  ): mutable.Queue[TimelineEvent] = {
    def produceLongStream(i: Long): Stream[Long] = {
      i #:: produceLongStream(i + step)
    }
    val steps =
      if (this.historical.isEmpty) {
        Stream.Empty
      } else {
        produceLongStream(start) takeWhile {
          timestamp => timestamp <= this.historical.last._1 + step
        }
      }

    steps.foldLeft(
      mutable.Queue[TimelineEvent](),
      this.historical
    ) {
      case ((res, remaining), nextStep) =>
        val (toConsider, rest) = remaining partition {
          case (timestamp, _) => timestamp <= nextStep
        }
        val timelineEvent = if (replaceTimestamp) {
          (nextStep, agg(toConsider)._2)
        } else {
          agg(toConsider)
        }

        res.enqueue(timelineEvent)
        (res, rest)
    }._1
  }

  /** Write the outcome timeline of the archive.
    *
    * @param header           header of the CSV file
    * @param start            first timestamp of the timeline
    * @param step             step of the timeline
    * @param agg              aggregation function to create the timeline
    * @param replaceTimestamp parameter which determines if the timeline uses
    *                         the discreet time step
    * @param toCsvLine        function which convert a timeline event to a CSV
    *                         line
    */
  def writeTimelineCSV(
    header: String,
    start: Long,
    step: Long,
    agg: mutable.Queue[Record] => TimelineEvent,
    replaceTimestamp: Boolean,
    toCsvLine: U => String
  ): Unit = {
    val completeHeader = "Timestamp;" + header + "\n"
    val timeline = this.createTimeline(start, step, agg, replaceTimestamp)
    val stringTimeline = Iterator(completeHeader) ++ timeline.toIterator.map {
      case (timestamp, timelineEvent) => timestamp + ";" + toCsvLine(timelineEvent) + "\n"
    }
    val timelineFileName =
      this.config.resultPath + "timeline_" + this.name + ".csv"
    val file = new File(timelineFileName)

    FileWriter.writeWith(file, stringTimeline)
  }

}
