package utils.jobs.netflix

import utils.jobs.MapJob

/** Map job to have the number of records by day and month on the netflix data set. */
class RecordByDayMonthMapJob extends MapJob {

  type K = String

  type V = Int

  /** @see utils.jobs.MapJob.map() */
  protected def map(line: String): Iterator[(String, Int)] = {
    val splitLine = line split ","
    val date = splitLine(3) split "-"
    val month = date(1)
    val day = date(2)
      Iterator((month+ " " + day, 1))
    }
}
