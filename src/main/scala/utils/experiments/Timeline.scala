package utils.experiments

import utils.files.FileWriter

import java.io.File
import scala.collection.mutable

/** Build a timeline from a history.
  *
  * @tparam R type of the records of the History
  * @tparam T type of the aggregation of records
  */
abstract class TimelineBuilder[R, T] {

  /** Aggregate records.
    *
    * @param records records to aggregate
    * @return aggregation of the records
    */
  def aggregateRecords(records: List[R]): T

  /** Build a timeline from a history.
    *
    * @param start   first timestamp to consider in the history
    * @param step    time step of the timeline
    * @param history history to build the timeline from
    * @return timeline extracted from <code>history</code>
    */
  def buildTimeline(
    start: Long,
    step: Long,
    history: History[R]
  ): Timeline[T] = {
    def produceLongStream(i: Long): Stream[Long] =
      i #:: produceLongStream(i + step)

    if (history.isEmpty) {
      new Timeline[T]()
    } else {
      val steps = 0.toLong #:: produceLongStream(start) takeWhile {
        timestamp => (timestamp <= history.lastTimestamp + step)
      }

      steps.zip(steps.tail).foldLeft(new Timeline[T]()) {
        case (timeline, (innerStart, stop)) =>
          val records = history.getRecordsBetween(innerStart, stop)
          val event = this aggregateRecords records

          timeline addEvent event
          timeline
      }
    }
  }

}

/** Represent a timeline.
  *
  * A timeline is a chronological aggregation of events.
  *
  * @param events events of the timeline
  * @tparam T     type of the events of the timeline
  */
class Timeline[T](events: mutable.Queue[T] = mutable.Queue[T]()) {

  /** Add event to the timeline.
    *
    * @param event event to add to the timeline.
    */
  def addEvent(event: T): Unit = this.events enqueue event

  /** Associate labels to each step of the timeline.
    *
    * @return list of labels, one label for each step of the timeline
    */
  protected def stepLabels: List[String] =
    ((0 to this.events.length) map { _.toString }).toList

  /** Write a CSV file from the timeline.
    *
    * @param filePath       path of the CSV file
    * @param eventHeader    header of the CSV file
    * @param eventToCsvLine function which build a CSV line from an event
    */
  def writeCsv(
    filePath: String,
    eventHeader: String,
    eventToCsvLine: T => String
  ): Unit = {
    val completeHeader = "Label;" + eventHeader
    val labeledEvents = (this.stepLabels zip this.events) map {
      case (label, event) => label + ";" + eventToCsvLine(event)
    }
    val csvContent = Iterator(completeHeader) ++ labeledEvents.toIterator
    val file = new File(filePath)

    FileWriter.writeWith(file, csvContent)
  }

}
