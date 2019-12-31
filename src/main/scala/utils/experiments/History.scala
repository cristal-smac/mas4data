package utils.experiments

import utils.files.FileWriter

import java.io.File
import scala.collection.mutable
import scala.io.Source

/** Allow agents to record events during a run.
  *
  * @param initialRecords initial records of the history
  * @tparam R             type of the records contained in this history
  */
class History[R](
  initialRecords: mutable.Queue[(Long, R)] = mutable.Queue[(Long, R)]()
) {

  // Represent a record with its associated timestamp
  type TimedRecord = (Long, R)

  // Sort records of the history
  private def sortRecords(r1: TimedRecord, r2: TimedRecord): Boolean = {
    val (t1, _) = r1
    val (t2, _) = r2

    t1 < t2
  }

  // Inner history
  private val history: mutable.Queue[TimedRecord] =
    this.initialRecords sortWith this.sortRecords

  /** Secondary constructor. Build an history from a file.
    *
    * @param filePath     path of the file which contains the previously wrote
    *                     history
    * @param lineToRecord function to build a time record from a string
    */
  def this(filePath: String, lineToRecord: String => (Long, R)) = {
    this(
      Source.fromFile(filePath)
            .getLines
            .drop(1)
            .map(lineToRecord)
            .foldLeft(mutable.Queue[(Long, R)]()) {
        case (acc, record) =>
          acc.enqueue(record)
          acc
      }
    )
  }

  /** Return <code>true</code> if the history is empty.
    *
    * @return <code>true</code> iff the history is empty
    */
  def isEmpty: Boolean = this.history.isEmpty

  /** Return the last timestamp of the history.
    *
    * @return last timestamp of the history
    */
  def lastTimestamp: Long = this.history.last._1

  /** Return all the records between two given timestamps.
    *
    * @param start first timestamp
    * @param stop  last timestamp
    * @return      all the records between <code>start</code> and
    *              <code>stop</code>
    */
  def getRecordsBetween(start: Long, stop: Long): List[R] =
    this.history.dropWhile({ case (t, _) => t < start })
                .takeWhile({ case (t, _) => t <= stop })
                .map(_._2)
                .toList

  /** Add a record to the history.
    *
    * @param record record to add to the history
    */
  def addRecord(record: R): Unit = {
    val timestamp = System.currentTimeMillis

    this.history.enqueue((timestamp, record))
  }

  /** Fuse the history with an other given history.
    *
    * @param h other history to fuse the history with
    * @return fusion of the history with <code>h</code>
    */
  def fuseWith(h: History[R]): History[R] =
    new History((this.history ++ h.history) sortWith this.sortRecords)

  /** Write a CSV file from the history.
    *
    * @param filePath        path of the CSV file to write
    * @param recordHeader    header of the CSV file
    * @param recordToCsvLine function which builds a CSV line from a time record
    */
  def writeCsv(
    filePath: String,
    recordHeader: String,
    recordToCsvLine: R => String
  ): Unit = {
    val file = new File(filePath)
    val stringHistory = this.history.toIterator map {
      case (timestamp, record) => timestamp + ";" + recordToCsvLine(record)
    }
    val completeHeader = "Timestamp;" + recordHeader

    FileWriter.writeWith(file, Iterator(completeHeader) ++ stringHistory)
  }

}
