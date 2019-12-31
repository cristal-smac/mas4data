package utils.jobs.netflix

import utils.jobs.MapJob

/** Map job to have the number of records by film on the netflix data set.
  */
class RecordByFilmMapJob extends MapJob {

  type K = Double

  type V = Int

  protected def map(line: String): Iterator[(Double, Int)] = {
    val splitLine = line split ","
    val idFilm = splitLine(0).toDouble
    Iterator((idFilm, 1))
  }
}
