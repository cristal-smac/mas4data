package utils.jobs.meteo

import utils.jobs.MapJob

/** Map job to have the number of records by year and station on the meteo data set. */
class RecordByYearStationMapJob extends MapJob {

  type K = String

  type V = Int

  /** @see utils.jobs.MapJob.map() */
  protected def map(line: String): Iterator[(String, Int)] = {
    val splitLine = line split ";"
    val year  = splitLine(1).substring(4)
    val station = splitLine(0)
    Iterator((year + " " + station, 1))
  }
}
